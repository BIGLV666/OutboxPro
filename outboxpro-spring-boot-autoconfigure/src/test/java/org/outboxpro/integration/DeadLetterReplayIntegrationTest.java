package org.outboxpro.integration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.outboxpro.autoconfigure.DeadLetterReplayEndpoint;
import org.outboxpro.core.OutboxProPublisher;
import org.outboxpro.core.context.EventContext;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.handler.OutboxProHandler;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.core.subscription.ConsumeMode;
import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.outboxpro.spi.deadletter.DeadLetterAlertNotifier;
import org.outboxpro.spi.deadletter.DeadLetterContext;
import org.outboxpro.spi.deadletter.DeadLetterRepository;
import org.outboxpro.spi.deadletter.DlqReplayAuthorizer;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 清单 T28–T34：DLQ 台账、人工重放、分桶计数器与告警（FRAMEWORK 模式）。
 *
 * <p>完整链路：Handler 失败 → 重试耗尽 → DLQ + 死信台账 → 人工重放（记录操作人与原因、
 * 幂等认领、次数限制）→ 消息重新投递并被成功消费 → 台账状态与分桶计数器同步 →
 * 积压告警与恢复通知。</p>
 *
 * <p>重放直接调用 {@link DeadLetterReplayEndpoint} 方法（等价于 HTTP 入口），
 * 授权与审计逻辑保持不变。告警阈值 2 / 恢复阈值 1 / 轮询 100ms，保证用例秒级完成。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, DeadLetterReplayIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=200ms",
                "outboxpro.consumer.concurrency=1",
                "outboxpro.dlq.replay.enabled=true",
                "outboxpro.dlq.ledger.max-replay-count=1",
                // 告警任务在独立测试类中验证（其内部 alertFired 状态跨用例共享，无法在本类复位）
                "outboxpro.dlq.alert.enabled=false"
        })
class DeadLetterReplayIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库，避免其他上下文的 Relay 认领本类留下的记录。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "dlq");
    }

    private static final String EVENT_TYPE = "it.dlq.created";
    private static final String EXCHANGE = "it.dlq.exchange";
    private static final String QUEUE = "it.dlq.queue";
    private static final String CONSUMER_NAME = "it-dlq-consumer";

    /** Handler 执行次数（按 orderId）。 */
    static final ConcurrentHashMap<Long, Integer> INVOCATIONS = new ConcurrentHashMap<>();
    /** 始终失败的订单集合；从集合移除后重放即可成功。 */
    static final Set<Long> ALWAYS_FAIL = ConcurrentHashMap.newKeySet();

    /** 告警通知记录。 */
    static final AtomicLong HIGH_WATERMARK_CALLS = new AtomicLong();
    static final AtomicLong RECOVERED_CALLS = new AtomicLong();
    static final List<DeadLetterContext> NOTIFY_CALLS = new CopyOnWriteArrayList<>();

    @Autowired
    OutboxProPublisher publisher;

    @Autowired
    DeadLetterReplayEndpoint replayEndpoint;

    @Autowired
    DeadLetterRepository deadLetterRepository;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    JdbcTemplate jdbc;

    /** 测试专用配置：事件、订阅、Handler、授权器与告警通知器。 */
    @Configuration
    static class Config {

        @Bean
        EventDefinition<OrderCreatedPayload> dlqEventDefinition() {
            return EventDefinition.<OrderCreatedPayload>builder()
                    .eventType(EVENT_TYPE)
                    .payloadType(OrderCreatedPayload.class)
                    .route(EXCHANGE, EVENT_TYPE)
                    .build();
        }

        @Bean
        OutboxProSubscription dlqSubscription() {
            EventBinding binding = new EventBinding(EVENT_TYPE, EVENT_TYPE, OrderCreatedPayload.class,
                    ConsumeMode.RELIABLE, new RetryPolicy(true, 2, Duration.ofMillis(100), 1, Duration.ofMillis(500)));
            return OutboxProSubscription.builder()
                    .name("it-dlq")
                    .exchange(EXCHANGE)
                    .queue(QUEUE)
                    .consumerName(CONSUMER_NAME)
                    .bindings(binding)
                    .build();
        }

        @Bean
        OutboxProHandler<OrderCreatedPayload> dlqHandler() {
            return new OutboxProHandler<>() {
                @Override
                public String eventType() {
                    return EVENT_TYPE;
                }

                @Override
                public Class<OrderCreatedPayload> payloadType() {
                    return OrderCreatedPayload.class;
                }

                @Override
                public void handle(EventContext<OrderCreatedPayload> context) {
                    long orderId = context.getPayload().orderId();
                    INVOCATIONS.merge(orderId, 1, Integer::sum);
                    if (ALWAYS_FAIL.contains(orderId)) {
                        throw new IllegalStateException("order " + orderId + " fails on purpose");
                    }
                }
            };
        }

        /** 授权器：拒绝测试用"未授权操作员"，其余放行。 */
        @Bean
        DlqReplayAuthorizer testReplayAuthorizer() {
            return (eventId, operator) -> {
                if ("hacker".equals(operator)) {
                    throw new SecurityException("operator not allowed to replay " + eventId);
                }
            };
        }

        /** 告警通知器：只记录调用，供用例断言。 */
        @Bean
        DeadLetterAlertNotifier recordingAlertNotifier() {
            return new DeadLetterAlertNotifier() {
                @Override
                public void onHighWatermark(long pendingCount, long threshold) {
                    HIGH_WATERMARK_CALLS.incrementAndGet();
                }

                @Override
                public void onRecovered(long pendingCount, long recoveryThreshold) {
                    RECOVERED_CALLS.incrementAndGet();
                }

                @Override
                public void notify(DeadLetterContext context) {
                    NOTIFY_CALLS.add(context);
                }
            };
        }
    }

    @BeforeEach
    void resetState() {
        INVOCATIONS.clear();
        ALWAYS_FAIL.clear();
        HIGH_WATERMARK_CALLS.set(0);
        RECOVERED_CALLS.set(0);
        NOTIFY_CALLS.clear();
    }

    /**
     * 清理消息表但保留台账行：分桶计数器由仓储代码在状态迁移时维护，
     * 原始 DELETE 会绕过桶维护造成计数漂移；台账只增不删，断言全部使用相对值。
     */
    @AfterEach
    void cleanMessageTables() {
        jdbc.update("DELETE FROM outboxpro_inbox WHERE consumer_name = ?", CONSUMER_NAME);
        jdbc.update("DELETE FROM outboxpro_outbox WHERE event_type = ?", EVENT_TYPE);
    }

    /** 发布一个注定死信的事件并等待其进入台账 PENDING_REPLAY。 */
    private String produceDeadLetter(long orderId) {
        ALWAYS_FAIL.add(orderId);
        var envelope = publisher.publish(EVENT_TYPE, new OrderCreatedPayload(orderId));
        // 用 queryForList + hasSize 断言：行未写入时抛 AssertionError 让 Awaitility 重试，
        // 而 queryForMap 抛的 EmptyResultDataAccessException 会直接终止等待。
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var rows = jdbc.queryForList(
                    "SELECT status, reason_code, attempt_count, payload_json FROM outboxpro_dead_letter "
                            + "WHERE event_id = ? AND consumer_name = ?",
                    envelope.getEventId(), CONSUMER_NAME);
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).get("status")).isEqualTo("PENDING_REPLAY");
        });
        return envelope.getEventId();
    }

    /** T28：死信写入台账，原因、尝试次数、载荷、订阅信息完整。 */
    @Test
    void deadLetterWritesCompleteLedgerRecord() {
        String eventId = produceDeadLetter(51001L);

        Map<String, Object> ledger = jdbc.queryForMap(
                "SELECT reason_code, attempt_count, payload_json, queue_name, original_exchange, original_routing_key "
                        + "FROM outboxpro_dead_letter WHERE event_id = ? AND consumer_name = ?",
                eventId, CONSUMER_NAME);
        assertThat(ledger.get("reason_code")).isEqualTo("RETRY_EXHAUSTED");
        assertThat((Integer) ledger.get("attempt_count")).isEqualTo(2);
        assertThat((String) ledger.get("payload_json")).contains(eventId).contains("51001");
        assertThat(ledger.get("queue_name")).isEqualTo(QUEUE);
        assertThat(ledger.get("original_exchange")).isEqualTo(EXCHANGE);
        assertThat(ledger.get("original_routing_key")).isEqualTo(EVENT_TYPE);
        // 框架同时把死信副本发布到了 RabbitMQ DLQ
        assertThat(rabbitTemplate.receive(QUEUE + ".dlq", Duration.ofSeconds(3).toMillis())).isNotNull();
    }

    /** T29：人工重放 → 消息重新投递 → Handler 成功 → 台账 REPLAYED 且记录操作人与原因。 */
    @Test
    void replayRepublishesAndUpdatesLedger() {
        long pendingBefore = deadLetterRepository.pendingReplayCount();
        String eventId = produceDeadLetter(51002L);
        assertThat(deadLetterRepository.pendingReplayCount()).isEqualTo(pendingBefore + 1);

        // 运维修复后重放：Handler 不再失败
        ALWAYS_FAIL.remove(51002L);
        var result = replayEndpoint.replayByEventId(eventId,
                new DeadLetterReplayEndpoint.ReplayRequest("ops-alice", "bug fixed, redeploy"));

        assertThat(result.replayedCount()).isEqualTo(1);
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            String inbox = jdbc.queryForObject(
                    "SELECT status FROM outboxpro_inbox WHERE consumer_name = ? AND event_id = ?",
                    String.class, CONSUMER_NAME, eventId);
            assertThat(inbox).as("重放后 Handler 应成功消费并写 Inbox").isEqualTo("SUCCESS");
        });

        Map<String, Object> ledger = jdbc.queryForMap(
                "SELECT status, replay_count, last_replay_operator, last_replay_reason "
                        + "FROM outboxpro_dead_letter WHERE event_id = ? AND consumer_name = ?",
                eventId, CONSUMER_NAME);
        assertThat(ledger.get("status")).isEqualTo("REPLAYED");
        assertThat((Integer) ledger.get("replay_count")).isEqualTo(1);
        assertThat(ledger.get("last_replay_operator")).isEqualTo("ops-alice");
        assertThat(ledger.get("last_replay_reason")).isEqualTo("bug fixed, redeploy");
        // 台账按设计只增不删（其他用例会留下 PENDING_REPLAY 行），断言使用相对值：
        // 本条死信写入时 +1，重放认领时 -1，总量应回到测试开始前的水平。
        assertThat(deadLetterRepository.pendingReplayCount()).isEqualTo(pendingBefore);
    }

    /** T30：重放次数超过 max-replay-count 后被拒绝。 */
    @Test
    void replayBeyondMaxCountIsRejected() {
        String eventId = produceDeadLetter(51003L);
        ALWAYS_FAIL.remove(51003L);

        var first = replayEndpoint.replayByEventId(eventId,
                new DeadLetterReplayEndpoint.ReplayRequest("ops-alice", "first replay"));
        assertThat(first.replayedCount()).isEqualTo(1);

        var second = replayEndpoint.replayByEventId(eventId,
                new DeadLetterReplayEndpoint.ReplayRequest("ops-alice", "second replay attempt"));
        assertThat(second.replayedCount()).as("超过 max-replay-count=1 应拒绝重放").isZero();
        assertThat(second.message()).contains("max replay count");
    }

    /** T31：并发重放同一 eventId → 台账租约保证只成功一次。 */
    @Test
    void concurrentReplayIsClaimedExactlyOnce() throws Exception {
        String eventId = produceDeadLetter(51004L);
        ALWAYS_FAIL.remove(51004L);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger totalReplayed = new AtomicInteger();
        for (int i = 0; i < 2; i++) {
            String operator = "ops-" + i;
            pool.submit(() -> {
                try {
                    start.await();
                    var result = replayEndpoint.replayByEventId(eventId,
                            new DeadLetterReplayEndpoint.ReplayRequest(operator, "concurrent replay drill"));
                    totalReplayed.addAndGet(result.replayedCount());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(15, TimeUnit.SECONDS)).isTrue();

        assertThat(totalReplayed.get()).as("两个并发重放请求合计只应成功一次").isEqualTo(1);
        Integer replayCount = jdbc.queryForObject(
                "SELECT replay_count FROM outboxpro_dead_letter WHERE event_id = ? AND consumer_name = ?",
                Integer.class, eventId, CONSUMER_NAME);
        assertThat(replayCount).isEqualTo(1);
    }

    /** T32：未授权操作员触发 SecurityException，台账不受影响。 */
    @Test
    void unauthorizedOperatorIsRejected() {
        String eventId = produceDeadLetter(51005L);

        assertThatThrownBy(() -> replayEndpoint.replayByEventId(eventId,
                new DeadLetterReplayEndpoint.ReplayRequest("hacker", "unauthorized attempt")))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("not allowed");

        // 授权在认领之前执行，台账应保持 PENDING_REPLAY
        String status = jdbc.queryForObject(
                "SELECT status FROM outboxpro_dead_letter WHERE event_id = ? AND consumer_name = ?",
                String.class, eventId, CONSUMER_NAME);
        assertThat(status).isEqualTo("PENDING_REPLAY");
    }

    /** T33：分桶计数器与台账明细对账一致（写入 +1、重放 -1、无漂移）。 */
    @Test
    void bucketCounterStaysConsistentWithLedger() {
        long before = deadLetterRepository.pendingReplayCount();

        String eventId = produceDeadLetter(51006L);
        long afterWrite = deadLetterRepository.pendingReplayCount();
        assertThat(afterWrite).isEqualTo(before + 1);
        assertThat(afterWrite).isEqualTo(sqlPendingCount());

        ALWAYS_FAIL.remove(51006L);
        replayEndpoint.replayByEventId(eventId,
                new DeadLetterReplayEndpoint.ReplayRequest("ops-alice", "counter drill"));

        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() ->
                deadLetterRepository.pendingReplayCount() == before);
        assertThat(deadLetterRepository.pendingReplayCount()).isEqualTo(sqlPendingCount());
    }

    /** 台账中仍可重放（PENDING_REPLAY）的明细条数。 */
    private long sqlPendingCount() {
        Long count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM outboxpro_dead_letter WHERE status = 'PENDING_REPLAY' AND consumer_name = ?",
                Long.class, CONSUMER_NAME);
        return count == null ? 0 : count;
    }
}
