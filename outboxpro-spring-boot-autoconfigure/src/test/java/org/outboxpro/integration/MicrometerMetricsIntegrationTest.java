package org.outboxpro.integration;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.outboxpro.core.OutboxProPublisher;
import org.outboxpro.core.context.EventContext;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.handler.OutboxProHandler;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.core.subscription.ConsumeMode;
import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 清单 P1.3：Micrometer 指标。
 *
 * <p>验证核心指标随真实消息流转递增：publish.total/success、relay.claimed、
 * consume.total/success、失败路径的 consume.retry / consume.dead、
 * 重复投递的 inbox.duplicate，以及低基数标签（event_type / producer / consumer / queue）。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, MicrometerMetricsIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=200ms",
                "outboxpro.consumer.concurrency=1"
        })
class MicrometerMetricsIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库，避免其他上下文的 Relay 认领本类留下的记录。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "metrics");
    }

    private static final String EVENT_TYPE = "it.metrics.created";
    private static final String QUEUE = "it.metrics.queue";
    private static final String CONSUMER_NAME = "it-metrics-consumer";

    static final Set<Long> ALWAYS_FAIL = ConcurrentHashMap.newKeySet();

    @Autowired
    OutboxProPublisher publisher;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    JdbcTemplate jdbc;

    /** 测试专用配置：SimpleMeterRegistry + 事件/订阅/Handler。 */
    @Configuration
    static class Config {

        @Bean
        SimpleMeterRegistry simpleMeterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        EventDefinition<OrderCreatedPayload> metricsEventDefinition() {
            return EventDefinition.<OrderCreatedPayload>builder()
                    .eventType(EVENT_TYPE)
                    .payloadType(OrderCreatedPayload.class)
                    .route("it.metrics.exchange", EVENT_TYPE)
                    .build();
        }

        @Bean
        OutboxProSubscription metricsSubscription() {
            EventBinding binding = new EventBinding(EVENT_TYPE, EVENT_TYPE, OrderCreatedPayload.class,
                    ConsumeMode.RELIABLE, new RetryPolicy(true, 2, Duration.ofMillis(100), 1, Duration.ofMillis(500)));
            return OutboxProSubscription.builder()
                    .name("it-metrics")
                    .exchange("it.metrics.exchange")
                    .queue(QUEUE)
                    .consumerName(CONSUMER_NAME)
                    .bindings(binding)
                    .build();
        }

        @Bean
        OutboxProHandler<OrderCreatedPayload> metricsHandler() {
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
                        throw new IllegalStateException("metrics drill failure");
                    }
                }
            };
        }
    }

    @AfterEach
    void resetState() {
        ALWAYS_FAIL.clear();
        jdbc.update("DELETE FROM outboxpro_inbox WHERE consumer_name = ?", CONSUMER_NAME);
        jdbc.update("DELETE FROM outboxpro_outbox WHERE event_type = ?", EVENT_TYPE);
    }

    /** 双精度计数器读取，指标尚未注册时返回 0。 */
    private double counter(String name, String... tags) {
        try {
            return meterRegistry.get(name).tags(tags).counter().count();
        } catch (RuntimeException missing) {
            // Micrometer 不同版本对缺失 meter 抛出的异常类型不同，统一按 0 处理
            return 0;
        }
    }

    /** P1.3：成功链路的生产端与消费端指标。 */
    @Test
    void successPathCounters() {
        publisher.publish(EVENT_TYPE, new OrderCreatedPayload(55001L));

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(counter("outboxpro.publish.total", "event_type", EVENT_TYPE, "producer", "application"))
                    .isGreaterThanOrEqualTo(1);
            assertThat(counter("outboxpro.publish.success", "event_type", EVENT_TYPE, "producer", "application"))
                    .isGreaterThanOrEqualTo(1);
            assertThat(counter("outboxpro.relay.claimed")).isGreaterThanOrEqualTo(1);
            assertThat(counter("outboxpro.consume.total", "event_type", EVENT_TYPE, "consumer", CONSUMER_NAME,
                    "queue", QUEUE)).isGreaterThanOrEqualTo(1);
            assertThat(counter("outboxpro.consume.success", "event_type", EVENT_TYPE, "consumer", CONSUMER_NAME,
                    "queue", QUEUE)).isGreaterThanOrEqualTo(1);
        });
    }

    /** P1.3：失败路径的 consume.retry 与重试耗尽后的 consume.dead。 */
    @Test
    void failurePathCounters() {
        ALWAYS_FAIL.add(55002L);
        publisher.publish(EVENT_TYPE, new OrderCreatedPayload(55002L));

        Awaitility.await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            assertThat(counter("outboxpro.consume.retry", "event_type", EVENT_TYPE, "consumer", CONSUMER_NAME,
                    "queue", QUEUE)).as("第一次失败应进入重试").isGreaterThanOrEqualTo(1);
            assertThat(counter("outboxpro.consume.dead", "event_type", EVENT_TYPE, "consumer", CONSUMER_NAME,
                    "queue", QUEUE)).as("重试耗尽应进入死信").isGreaterThanOrEqualTo(1);
            assertThat(counter("outboxpro.consume.failure", "event_type", EVENT_TYPE, "consumer", CONSUMER_NAME,
                    "queue", QUEUE)).isGreaterThanOrEqualTo(2);
        });
    }

    /** P1.3：重复投递触发 inbox.duplicate，且不重复计入 consume.total。 */
    @Test
    void duplicateDeliveryCounter() {
        var envelope = publisher.publish(EVENT_TYPE, new OrderCreatedPayload(55003L));
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(counter("outboxpro.consume.success", "event_type", EVENT_TYPE, "consumer", CONSUMER_NAME,
                        "queue", QUEUE)).isGreaterThanOrEqualTo(1));

        double duplicateBefore = counter("outboxpro.inbox.duplicate",
                "event_type", EVENT_TYPE, "consumer", CONSUMER_NAME);

        // 模拟上游重复投递同一 Envelope
        String envelopeJson = jdbc.queryForObject(
                "SELECT payload_json FROM outboxpro_outbox WHERE event_id = ?", String.class, envelope.getEventId());
        rabbitTemplate.convertAndSend("it.metrics.exchange", EVENT_TYPE, envelopeJson.getBytes(StandardCharsets.UTF_8));

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(counter("outboxpro.inbox.duplicate", "event_type", EVENT_TYPE, "consumer", CONSUMER_NAME))
                        .as("重复投递应计入 inbox.duplicate")
                        .isEqualTo(duplicateBefore + 1));
    }
}
