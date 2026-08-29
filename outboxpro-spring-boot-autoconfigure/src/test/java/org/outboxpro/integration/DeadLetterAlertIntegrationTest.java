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
import org.outboxpro.spi.deadletter.DlqReplayAuthorizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 清单 T34：死信积压告警生命周期。
 *
 * <p>告警任务是有状态单例（alertFired / 冷却时间），因此使用独立上下文与数据库，
 * 保证测试从全新状态开始：两条死信使积压达到阈值触发一次高位告警；
 * 冷却期内不重复触发；人工重放使积压降到恢复阈值以下后发送恢复通知。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, DeadLetterAlertIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=200ms",
                "outboxpro.consumer.concurrency=1",
                "outboxpro.dlq.replay.enabled=true",
                "outboxpro.dlq.alert.poll-interval=100ms",
                "outboxpro.dlq.alert.threshold=2",
                "outboxpro.dlq.alert.recovery-threshold=1",
                "outboxpro.dlq.alert.cooldown=200ms"
        })
class DeadLetterAlertIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库；告警任务的状态与台账数据都必须从零开始。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "alert");
    }

    private static final String EVENT_TYPE = "it.alert.created";
    private static final String EXCHANGE = "it.alert.exchange";
    private static final String QUEUE = "it.alert.queue";
    private static final String CONSUMER_NAME = "it-alert-consumer";

    static final Set<Long> ALWAYS_FAIL = ConcurrentHashMap.newKeySet();
    static final AtomicLong HIGH_WATERMARK_CALLS = new AtomicLong();
    static final AtomicLong RECOVERED_CALLS = new AtomicLong();

    @Autowired
    OutboxProPublisher publisher;

    @Autowired
    DeadLetterReplayEndpoint replayEndpoint;

    @Autowired
    JdbcTemplate jdbc;

    /** 测试专用配置：事件、订阅、Handler 与记录型告警通知器。 */
    @Configuration
    static class Config {

        @Bean
        EventDefinition<OrderCreatedPayload> alertEventDefinition() {
            return EventDefinition.<OrderCreatedPayload>builder()
                    .eventType(EVENT_TYPE)
                    .payloadType(OrderCreatedPayload.class)
                    .route(EXCHANGE, EVENT_TYPE)
                    .build();
        }

        @Bean
        OutboxProSubscription alertSubscription() {
            EventBinding binding = new EventBinding(EVENT_TYPE, EVENT_TYPE, OrderCreatedPayload.class,
                    ConsumeMode.RELIABLE, new RetryPolicy(true, 2, Duration.ofMillis(100), 1, Duration.ofMillis(500)));
            return OutboxProSubscription.builder()
                    .name("it-alert")
                    .exchange(EXCHANGE)
                    .queue(QUEUE)
                    .consumerName(CONSUMER_NAME)
                    .bindings(binding)
                    .build();
        }

        @Bean
        OutboxProHandler<OrderCreatedPayload> alertHandler() {
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
                    if (ALWAYS_FAIL.contains(context.getPayload().orderId())) {
                        throw new IllegalStateException("alert drill failure");
                    }
                }
            };
        }

        /** 放行所有重放请求，供积压恢复流程使用。 */
        @Bean
        DlqReplayAuthorizer allowAllAuthorizer() {
            return (eventId, operator) -> { };
        }

        @Bean
        DeadLetterAlertNotifier recordingNotifier() {
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
                    // 旁路通知不参与本用例断言
                }
            };
        }
    }

    @BeforeEach
    void resetCounters() {
        ALWAYS_FAIL.clear();
        HIGH_WATERMARK_CALLS.set(0);
        RECOVERED_CALLS.set(0);
    }

    /** 只清理消息表；台账行与分桶计数器必须保持一致，不做原始删除。 */
    @AfterEach
    void cleanMessageTables() {
        jdbc.update("DELETE FROM outboxpro_inbox WHERE consumer_name = ?", CONSUMER_NAME);
        jdbc.update("DELETE FROM outboxpro_outbox WHERE event_type = ?", EVENT_TYPE);
    }

    /** 发布一个注定死信的事件并等待其进入台账 PENDING_REPLAY。 */
    private String produceDeadLetter(long orderId) {
        ALWAYS_FAIL.add(orderId);
        var envelope = publisher.publish(EVENT_TYPE, new OrderCreatedPayload(orderId));
        // queryForList + hasSize：行未写入时抛 AssertionError 以便 Awaitility 重试
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var rows = jdbc.queryForList(
                    "SELECT status FROM outboxpro_dead_letter WHERE event_id = ? AND consumer_name = ?",
                    envelope.getEventId(), CONSUMER_NAME);
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).get("status")).isEqualTo("PENDING_REPLAY");
        });
        return envelope.getEventId();
    }

    /** T34：积压达阈值 → 高位告警一次 → 冷却期内不重复 → 重放后触发恢复通知。 */
    @Test
    void alertFiresOnBacklogAndRecoversAfterReplay() {
        // 两条死信使 pending=2 ≥ threshold=2，等待一次高位告警
        String eventA = produceDeadLetter(52001L);
        String eventB = produceDeadLetter(52002L);
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> HIGH_WATERMARK_CALLS.get() >= 1);

        // 冷却期内即使继续满足阈值也不重复告警
        long callsAfterFirst = HIGH_WATERMARK_CALLS.get();
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(HIGH_WATERMARK_CALLS.get()).as("冷却期内不得重复告警").isEqualTo(callsAfterFirst);

        // 重放两条死信，pending 降到恢复阈值(1)以下，等待恢复通知
        ALWAYS_FAIL.remove(52001L);
        ALWAYS_FAIL.remove(52002L);
        replayEndpoint.replayByEventId(eventA, new DeadLetterReplayEndpoint.ReplayRequest("ops-alice", "recover a"));
        replayEndpoint.replayByEventId(eventB, new DeadLetterReplayEndpoint.ReplayRequest("ops-bob", "recover b"));

        Awaitility.await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(10))
                .until(() -> RECOVERED_CALLS.get() >= 1);
        assertThat(HIGH_WATERMARK_CALLS.get()).as("整个生命周期只应触发一次高位告警").isEqualTo(1);
    }
}
