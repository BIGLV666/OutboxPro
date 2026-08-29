package org.outboxpro.integration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.outboxpro.core.OutboxProPublisher;
import org.outboxpro.core.context.EventContext;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.exception.NonRetryableEventException;
import org.outboxpro.core.exception.RetryableEventException;
import org.outboxpro.core.handler.OutboxProHandler;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.core.subscription.ConsumeMode;
import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 清单 T22–T27：异常分类与 BEST_EFFORT 消费。
 *
 * <p>验证框架的分流规则：
 * T22 NonRetryableEventException 跳过重试直接死信；
 * T23 RetryableEventException 走 Retry Queue 后恢复；
 * T24 未分类异常走重试，耗尽后死信原因为 HANDLER_FAILURE；
 * T25 BEST_EFFORT 失败记 IGNORED 并 ACK，不重试；
 * T26 未知事件类型入死信（UNKNOWN_EVENT_TYPE）；
 * T27 非法 JSON 入死信（MALFORMED_MESSAGE）。</p>
 *
 * <p>Handler 行为由 orderId 集合驱动：集合在发布前设置，避免与 200ms 轮询产生竞态。
 * 消费并发固定为 1，Reliable 绑定使用 maxAttempts=2 的快速重试策略。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, ConsumeFailureClassificationIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=200ms",
                "outboxpro.consumer.concurrency=1"
        })
class ConsumeFailureClassificationIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库，避免其他上下文的 Relay 认领本类留下的记录。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "classify");
    }

    private static final String PAY_EVENT = "it.pay.created";
    private static final String NOTIFY_EVENT = "it.notify.created";
    private static final String RELIABLE_QUEUE = "it.classify.reliable.queue";
    private static final String BEST_EFFORT_QUEUE = "it.classify.be.queue";
    private static final String RELIABLE_CONSUMER = "it-classify-reliable";
    private static final String BEST_EFFORT_CONSUMER = "it-classify-be";
    /** 绑定重试策略的 initialDelay：retry.1 的 TTL 即该值（delayForAttempt(1)）。 */
    private static final long RETRY_TTL_MILLIS = 100;

    /** 按 orderId 记录 Reliable Handler 执行次数。 */
    static final ConcurrentHashMap<Long, Integer> PAY_INVOCATIONS = new ConcurrentHashMap<>();
    /** 首次执行抛 RetryableEventException 后自动恢复的订单。 */
    static final Set<Long> RETRYABLE_ONCE = ConcurrentHashMap.newKeySet();
    /** 始终抛未分类异常的订单。 */
    static final Set<Long> ALWAYS_FAIL = ConcurrentHashMap.newKeySet();
    /** 始终抛 NonRetryableEventException 的订单。 */
    static final Set<Long> NON_RETRYABLE = ConcurrentHashMap.newKeySet();
    /** 首次执行时间，用于断言重试经过 TTL。 */
    static final Map<Long, Long> FIRST_NANOS = new ConcurrentHashMap<>();
    /** Best Effort Handler 执行次数。 */
    static final ConcurrentHashMap<Long, Integer> NOTIFY_INVOCATIONS = new ConcurrentHashMap<>();

    @Autowired
    OutboxProPublisher publisher;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    JdbcTemplate jdbc;

    /** 测试专用配置：两类事件、两条订阅（Reliable / Best Effort）与对应 Handler。 */
    @Configuration
    static class Config {

        @Bean
        EventDefinition<OrderCreatedPayload> payEventDefinition() {
            return EventDefinition.<OrderCreatedPayload>builder()
                    .eventType(PAY_EVENT)
                    .payloadType(OrderCreatedPayload.class)
                    .route("it.classify.exchange", PAY_EVENT)
                    .build();
        }

        @Bean
        EventDefinition<OrderCreatedPayload> notifyEventDefinition() {
            return EventDefinition.<OrderCreatedPayload>builder()
                    .eventType(NOTIFY_EVENT)
                    .payloadType(OrderCreatedPayload.class)
                    .route("it.classify.exchange", NOTIFY_EVENT)
                    .build();
        }

        @Bean
        OutboxProSubscription reliableSubscription() {
            // maxAttempts=2：第一次失败进 Retry Queue（TTL 1s），第二次失败直接 DLQ
            EventBinding binding = new EventBinding(PAY_EVENT, PAY_EVENT, OrderCreatedPayload.class,
                    ConsumeMode.RELIABLE, new RetryPolicy(true, 2, Duration.ofMillis(100), 1, Duration.ofMillis(500)));
            return OutboxProSubscription.builder()
                    .name("it-classify")
                    .exchange("it.classify.exchange")
                    .queue(RELIABLE_QUEUE)
                    .consumerName(RELIABLE_CONSUMER)
                    .bindings(binding)
                    .build();
        }

        @Bean
        OutboxProSubscription bestEffortSubscription() {
            EventBinding binding = EventBinding.bestEffort(NOTIFY_EVENT, NOTIFY_EVENT, OrderCreatedPayload.class);
            return OutboxProSubscription.builder()
                    .name("it-classify-be")
                    .exchange("it.classify.exchange")
                    .queue(BEST_EFFORT_QUEUE)
                    .consumerName(BEST_EFFORT_CONSUMER)
                    .bindings(binding)
                    .build();
        }

        @Bean
        OutboxProHandler<OrderCreatedPayload> payHandler() {
            return new OutboxProHandler<>() {
                @Override
                public String eventType() {
                    return PAY_EVENT;
                }

                @Override
                public Class<OrderCreatedPayload> payloadType() {
                    return OrderCreatedPayload.class;
                }

                @Override
                public void handle(EventContext<OrderCreatedPayload> context) {
                    long orderId = context.getPayload().orderId();
                    PAY_INVOCATIONS.merge(orderId, 1, Integer::sum);
                    FIRST_NANOS.putIfAbsent(orderId, System.nanoTime());
                    if (RETRYABLE_ONCE.remove(orderId)) {
                        throw new RetryableEventException("first attempt intentionally fails");
                    }
                    if (NON_RETRYABLE.contains(orderId)) {
                        throw new NonRetryableEventException("business rule violation");
                    }
                    if (ALWAYS_FAIL.contains(orderId)) {
                        throw new IllegalStateException("unclassified handler failure");
                    }
                }
            };
        }

        @Bean
        OutboxProHandler<OrderCreatedPayload> notifyHandler() {
            return new OutboxProHandler<>() {
                @Override
                public String eventType() {
                    return NOTIFY_EVENT;
                }

                @Override
                public Class<OrderCreatedPayload> payloadType() {
                    return OrderCreatedPayload.class;
                }

                @Override
                public void handle(EventContext<OrderCreatedPayload> context) {
                    long orderId = context.getPayload().orderId();
                    NOTIFY_INVOCATIONS.merge(orderId, 1, Integer::sum);
                    throw new IllegalStateException("best effort handler always fails");
                }
            };
        }
    }

    @BeforeEach
    void resetState() {
        PAY_INVOCATIONS.clear();
        RETRYABLE_ONCE.clear();
        ALWAYS_FAIL.clear();
        NON_RETRYABLE.clear();
        FIRST_NANOS.clear();
        NOTIFY_INVOCATIONS.clear();
    }

    /** 清理本消费者产生的死信台账与 Inbox 记录，避免共享数据库的跨用例干扰。 */
    @AfterEach
    void cleanLedger() {
        jdbc.update("DELETE FROM outboxpro_dead_letter WHERE consumer_name IN (?, ?)",
                RELIABLE_CONSUMER, BEST_EFFORT_CONSUMER);
    }

    /** T22：NonRetryableEventException → 不进重试，直接 DLQ + 台账 NON_RETRYABLE_EXCEPTION。 */
    @Test
    void nonRetryableExceptionGoesStraightToDlq() {
        NON_RETRYABLE.add(42001L);
        var envelope = publisher.publish(PAY_EVENT, new OrderCreatedPayload(42001L));

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var deadMessage = rabbitTemplate.receive(RELIABLE_QUEUE + ".dlq", Duration.ofSeconds(2).toMillis());
            assertThat(deadMessage).as("不可重试异常应直接进入 DLQ").isNotNull();
            assertThat(new String(deadMessage.getBody(), StandardCharsets.UTF_8)).contains(envelope.getEventId());
        });

        Map<String, Object> ledger = jdbc.queryForMap(
                "SELECT reason_code FROM outboxpro_dead_letter WHERE event_id = ? AND consumer_name = ?",
                envelope.getEventId(), RELIABLE_CONSUMER);
        assertThat(ledger.get("reason_code")).isEqualTo("NON_RETRYABLE_EXCEPTION");
        assertThat(PAY_INVOCATIONS).as("不可重试异常不应重试").containsEntry(42001L, 1);
    }

    /** T23：RetryableEventException → 走 Retry Queue，TTL 回流后恢复。 */
    @Test
    void retryableExceptionUsesRetryQueue() {
        RETRYABLE_ONCE.add(42002L);
        var envelope = publisher.publish(PAY_EVENT, new OrderCreatedPayload(42002L));

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(PAY_INVOCATIONS).containsEntry(42002L, 2));

        long elapsedMillis = (System.nanoTime() - FIRST_NANOS.get(42002L)) / 1_000_000;
        assertThat(elapsedMillis)
                .as("第二次执行必须经过 Retry Queue TTL 延迟")
                .isGreaterThanOrEqualTo(RETRY_TTL_MILLIS);
        // Handler 返回后本地事务才提交，Inbox SUCCESS 行的可见性需要等待
        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(inboxStatus(RELIABLE_CONSUMER, envelope.getEventId())).isEqualTo("SUCCESS"));
    }

    /** T24：未分类异常 → 重试耗尽 → DLQ，死信原因 HANDLER_FAILURE。 */
    @Test
    void unclassifiedFailureExhaustsToHandlerFailureDeadLetter() {
        ALWAYS_FAIL.add(42003L);
        var envelope = publisher.publish(PAY_EVENT, new OrderCreatedPayload(42003L));

        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(PAY_INVOCATIONS).containsEntry(42003L, 2));

        assertThat(receiveQuietly(RELIABLE_QUEUE + ".dlq")).as("耗尽后应进入 DLQ").isNotNull();

        Map<String, Object> ledger = jdbc.queryForMap(
                "SELECT reason_code FROM outboxpro_dead_letter WHERE event_id = ? AND consumer_name = ?",
                envelope.getEventId(), RELIABLE_CONSUMER);
        // 语义说明：重试耗尽是终态原因，无论原始异常是否可分类，耗尽后台账统一记 RETRY_EXHAUSTED；
        // HANDLER_FAILURE 保留给非耗尽的直接死信分派场景。
        assertThat(ledger.get("reason_code")).isEqualTo("RETRY_EXHAUSTED");
    }

    /** T25：BEST_EFFORT 失败 → Inbox IGNORED → ACK，不重试、不入 DLQ。 */
    @Test
    void bestEffortFailureIsIgnoredAndAcked() {
        var envelope = publisher.publish(NOTIFY_EVENT, new OrderCreatedPayload(42004L));

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(NOTIFY_INVOCATIONS).containsEntry(42004L, 1));

        // 等待足够时间确认不会出现第二次执行（不重试）
        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(NOTIFY_INVOCATIONS).containsEntry(42004L, 1);
        assertThat(inboxStatus(BEST_EFFORT_CONSUMER, envelope.getEventId())).isEqualTo("IGNORED");
        assertThat(receiveQuietly(BEST_EFFORT_QUEUE + ".dlq"))
                .as("BEST_EFFORT 失败不应发布到 DLQ").isNull();
    }

    /** T26：队列收到未声明的事件类型 → 死信 UNKNOWN_EVENT_TYPE。 */
    @Test
    void unknownEventTypeLandsInDeadLetterLedger() {
        String unknownEnvelope = """
                {"eventId":"unknown-event-1","eventType":"it.unknown.event","schemaVersion":"v1",
                 "producer":"tester","occurredAt":"2026-01-01T00:00:00Z","payload":{"orderId":1}}
                """;
        rabbitTemplate.convertAndSend("it.classify.exchange", PAY_EVENT,
                unknownEnvelope.getBytes(StandardCharsets.UTF_8));

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Map<String, Object> ledger = jdbc.queryForMap(
                    "SELECT reason_code FROM outboxpro_dead_letter WHERE event_id = 'unknown-event-1' AND consumer_name = ?",
                    RELIABLE_CONSUMER);
            assertThat(ledger.get("reason_code")).isEqualTo("UNKNOWN_EVENT_TYPE");
        });
    }

    /** T27：非法 JSON → 死信 MALFORMED_MESSAGE。 */
    @Test
    void malformedMessageLandsInDeadLetterLedger() {
        rabbitTemplate.convertAndSend("it.classify.exchange", PAY_EVENT,
                "this is not json".getBytes(StandardCharsets.UTF_8));

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Integer count = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM outboxpro_dead_letter WHERE reason_code = 'MALFORMED_MESSAGE' AND consumer_name = ?",
                    Integer.class, RELIABLE_CONSUMER);
            assertThat(count).isGreaterThanOrEqualTo(1);
        });
    }

    /** 查询 Inbox 状态。 */
    private String inboxStatus(String consumer, String eventId) {
        return jdbc.queryForObject(
                "SELECT status FROM outboxpro_inbox WHERE consumer_name = ? AND event_id = ?",
                String.class, consumer, eventId);
    }

    /** 带超时的静默 receive，超时返回 null。 */
    private Message receiveQuietly(String queue) {
        return rabbitTemplate.receive(queue, Duration.ofSeconds(2).toMillis());
    }
}
