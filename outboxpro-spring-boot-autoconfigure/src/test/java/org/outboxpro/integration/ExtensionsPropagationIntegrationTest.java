package org.outboxpro.integration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.outboxpro.core.OutboxProPublisher;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 回归测试：extensions 扩展元数据全链路传播。
 *
 * <p>payload_json 保存的是完整事件信封 JSON，extensions 嵌在消息体中随 Outbox、
 * RabbitMQ、重试副本与死信副本传播，消费端在 {@code invoke} 阶段还原进 EventEnvelope。
 * 该行为此前没有测试覆盖，本类锁住以下契约：</p>
 * <ol>
 *   <li>发布时传入的 extensions 在 Handler 的 {@code EventContext} 中原样可达；</li>
 *   <li>消息经历 Retry Queue 重试后，extensions 在下一次尝试中仍然可达。</li>
 * </ol>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, ExtensionsPropagationIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=200ms",
                "outboxpro.consumer.concurrency=1",
                "outboxpro.dlq.alert.enabled=false"
        })
class ExtensionsPropagationIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库，避免其他上下文的 Relay 认领本类留下的记录。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "ext");
    }

    private static final String EVENT_TYPE = "it.ext.created";
    private static final String EXCHANGE = "it.ext.exchange";
    private static final String QUEUE = "it.ext.queue";
    private static final String CONSUMER_NAME = "it-ext-consumer";

    /** 每次消费尝试收到的 extensions（按 orderId → 尝试顺序）。 */
    static final Map<Long, List<Map<String, Object>>> RECEIVED = new ConcurrentHashMap<>();
    /** 首次消费必须失败的订单集合。 */
    static final Set<Long> FAIL_ONCE = ConcurrentHashMap.newKeySet();

    @Autowired
    OutboxProPublisher publisher;

    /** 测试专用配置：事件、订阅与记录 extensions 的 Handler。 */
    @Configuration
    static class Config {

        @Bean
        EventDefinition<OrderCreatedPayload> extEventDefinition() {
            return EventDefinition.<OrderCreatedPayload>builder()
                    .eventType(EVENT_TYPE)
                    .payloadType(OrderCreatedPayload.class)
                    .route(EXCHANGE, EVENT_TYPE)
                    .build();
        }

        @Bean
        OutboxProSubscription extSubscription() {
            EventBinding binding = new EventBinding(EVENT_TYPE, EVENT_TYPE, OrderCreatedPayload.class,
                    ConsumeMode.RELIABLE, new RetryPolicy(true, 3, Duration.ofMillis(100), 1, Duration.ofSeconds(1)));
            return OutboxProSubscription.builder()
                    .name("it-ext")
                    .exchange(EXCHANGE)
                    .queue(QUEUE)
                    .consumerName(CONSUMER_NAME)
                    .bindings(binding)
                    .build();
        }

        @Bean
        OutboxProHandler<OrderCreatedPayload> extHandler() {
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
                    RECEIVED.computeIfAbsent(orderId, ignored -> new CopyOnWriteArrayList<>())
                            .add(context.getEnvelope().getExtensions());
                    if (FAIL_ONCE.remove(orderId)) {
                        throw new IllegalStateException("fail first attempt for " + orderId);
                    }
                }
            };
        }
    }

    /** extensions 随事件信封发布并原样到达 Handler。 */
    @Test
    void extensionsReachHandler() {
        Map<String, Object> extensions = Map.of("tenantId", "tenant-42", "source", "it");

        publisher.publish(EVENT_TYPE, new OrderCreatedPayload(9001L), extensions);

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<Map<String, Object>> attempts = RECEIVED.get(9001L);
            assertThat(attempts).as("Handler 应至少被调用一次").isNotEmpty();
            Map<String, Object> received = attempts.get(0);
            assertThat(received).containsEntry("tenantId", "tenant-42").containsEntry("source", "it");
        });
    }

    /** 消息经历 Retry Queue 重试后，extensions 在下一次尝试中保持不变。 */
    @Test
    void extensionsSurviveRetry() {
        long orderId = 9002L;
        FAIL_ONCE.add(orderId);

        publisher.publish(EVENT_TYPE, new OrderCreatedPayload(orderId), Map.of("tenantId", "tenant-42"));

        // 第一次尝试失败后进入 Retry Queue，重试副本中的 extensions 必须与原始消息一致。
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<Map<String, Object>> attempts = RECEIVED.get(orderId);
            assertThat(attempts).as("Handler 应经历两次尝试").hasSize(2);
            assertThat(attempts.get(0)).as("首次尝试应携带原始 extensions")
                    .containsEntry("tenantId", "tenant-42");
            assertThat(attempts.get(1)).as("重试副本应携带原始 extensions")
                    .containsEntry("tenantId", "tenant-42");
        });
    }
}
