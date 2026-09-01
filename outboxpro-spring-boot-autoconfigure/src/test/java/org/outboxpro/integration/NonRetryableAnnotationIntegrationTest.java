package org.outboxpro.integration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.outboxpro.core.OutboxProPublisher;
import org.outboxpro.core.annotation.NonRetryable;
import org.outboxpro.core.context.EventContext;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.handler.OutboxProHandler;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.core.subscription.ConsumeMode;
import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code @NonRetryable} 异常标注集成测试。
 *
 * <p>Handler 抛出标注了 {@code @NonRetryable} 的自定义业务异常时，框架跳过重试
 * 直接进入死信流程（与抛出 {@code NonRetryableEventException} 等价），
 * 死信原因记为 NON_RETRYABLE_EXCEPTION，台账 attempt_count 保持 1。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, NonRetryableAnnotationIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=200ms",
                "outboxpro.consumer.concurrency=1",
                "outboxpro.dlq.replay.enabled=true",
                "outboxpro.dlq.alert.enabled=false"
        })
class NonRetryableAnnotationIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库，避免其他上下文的 Relay 认领本类留下的记录。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "nonretry");
    }

    private static final String EVENT_TYPE = "it.nonretry.created";
    private static final String EXCHANGE = "it.nonretry.exchange";
    private static final String QUEUE = "it.nonretry.queue";
    private static final String CONSUMER_NAME = "it-nonretry-consumer";

    /** 标注 @NonRetryable 的业务异常：等价于 NonRetryableEventException。 */
    @NonRetryable
    static class InsufficientBalanceException extends RuntimeException {
        InsufficientBalanceException(String message) {
            super(message);
        }
    }

    /** Handler 执行次数（按 orderId）。 */
    static final Map<Long, Integer> INVOCATIONS = new ConcurrentHashMap<>();
    /** 必须抛出不可重试异常的订单集合。 */
    static final Set<Long> INSUFFICIENT_BALANCE = ConcurrentHashMap.newKeySet();

    @Autowired
    OutboxProPublisher publisher;

    @Autowired
    JdbcTemplate jdbc;

    /** 测试专用配置：重试预算充足（3 次），验证注解使框架跳过全部重试。 */
    @Configuration
    static class Config {

        @Bean
        EventDefinition<OrderCreatedPayload> nonRetryableEventDefinition() {
            return EventDefinition.<OrderCreatedPayload>builder()
                    .eventType(EVENT_TYPE)
                    .payloadType(OrderCreatedPayload.class)
                    .route(EXCHANGE, EVENT_TYPE)
                    .build();
        }

        @Bean
        OutboxProSubscription nonRetryableSubscription() {
            EventBinding binding = new EventBinding(EVENT_TYPE, EVENT_TYPE, OrderCreatedPayload.class,
                    ConsumeMode.RELIABLE, new RetryPolicy(true, 3, Duration.ofMillis(200), 1, Duration.ofSeconds(1)));
            return OutboxProSubscription.builder()
                    .name("it-nonretry")
                    .exchange(EXCHANGE)
                    .queue(QUEUE)
                    .consumerName(CONSUMER_NAME)
                    .bindings(binding)
                    .build();
        }

        @Bean
        OutboxProHandler<OrderCreatedPayload> nonRetryableHandler() {
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
                    if (INSUFFICIENT_BALANCE.contains(orderId)) {
                        throw new InsufficientBalanceException("balance not enough for order " + orderId);
                    }
                }
            };
        }
    }

    @AfterEach
    void resetState() {
        INVOCATIONS.clear();
        INSUFFICIENT_BALANCE.clear();
        jdbc.update("DELETE FROM outboxpro_inbox WHERE consumer_name = ?", CONSUMER_NAME);
        jdbc.update("DELETE FROM outboxpro_outbox WHERE event_type = ?", EVENT_TYPE);
    }

    /** 标注异常一次失败即进入死信台账，原因 NON_RETRYABLE_EXCEPTION，无重试发生。 */
    @Test
    void annotatedExceptionSkipsRetryAndGoesToDeadLetter() {
        long orderId = 9301L;
        INSUFFICIENT_BALANCE.add(orderId);
        var envelope = publisher.publish(EVENT_TYPE, new OrderCreatedPayload(orderId));

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var rows = jdbc.queryForList(
                    "SELECT status, reason_code, attempt_count FROM outboxpro_dead_letter "
                            + "WHERE event_id = ? AND consumer_name = ?",
                    envelope.getEventId(), CONSUMER_NAME);
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).get("reason_code")).isEqualTo("NON_RETRYABLE_EXCEPTION");
            assertThat((Integer) rows.get(0).get("attempt_count")).as("不可重试异常不应消耗重试").isEqualTo(1);
            assertThat(rows.get(0).get("status")).isEqualTo("PENDING_REPLAY");
        });

        // 等待一段时间确认没有重试副本再次执行 Handler。
        org.awaitility.Awaitility.await().pollDelay(Duration.ofSeconds(1)).until(() -> true);
        assertThat(INVOCATIONS.get(orderId)).as("标注异常应只执行一次").isEqualTo(1);
    }

    /** 未标注的普通异常仍然走重试路径，语义保持向后兼容。 */
    @Test
    void unannotatedExceptionStillRetries() {
        long orderId = 9302L;
        var envelope = publisher.publish(EVENT_TYPE, new OrderCreatedPayload(orderId));
        // Handler 不抛异常，消息应被成功消费（对照组）。
        // 用 queryForList + hasSize 让 Awaitility 在行未写入时持续重试。
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var rows = jdbc.queryForList(
                    "SELECT status FROM outboxpro_inbox WHERE consumer_name = ? AND event_id = ?",
                    CONSUMER_NAME, envelope.getEventId());
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).get("status")).isEqualTo("SUCCESS");
        });
        assertThat(INVOCATIONS.get(orderId)).isEqualTo(1);
    }
}
