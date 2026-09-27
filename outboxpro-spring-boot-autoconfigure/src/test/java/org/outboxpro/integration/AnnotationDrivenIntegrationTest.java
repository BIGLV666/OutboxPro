package org.outboxpro.integration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.outboxpro.core.OutboxProPublisher;
import org.outboxpro.core.annotation.OutboxEvent;
import org.outboxpro.core.annotation.OutboxHandler;
import org.outboxpro.core.context.EventContext;
import org.outboxpro.core.event.EventRegistry;
import org.outboxpro.core.exception.EventConfigurationException;
import org.outboxpro.core.handler.AnnotatedOutboxHandler;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 注解式装配集成测试：{@code @OutboxEvent} + {@code @OutboxHandler} 替代手写
 * EventDefinition / OutboxProSubscription Bean，并支持类型安全发布。
 *
 * <p>验证：</p>
 * <ol>
 *   <li>注解式 Handler 自动生成事件定义与订阅，消息可完整走通发布 → 消费；</li>
 *   <li>{@code publish(Class, payload)} 按载荷类型反查事件类型后正常发布；</li>
 *   <li>注册表按载荷类型反查在缺失或多义时快速失败。</li>
 * </ol>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, AnnotationDrivenIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=200ms",
                "outboxpro.consumer.concurrency=1",
                "outboxpro.retry.enabled=true",
                "outboxpro.retry.max-attempts=3",
                "outboxpro.retry.initial-delay=200ms",
                "outboxpro.dlq.alert.enabled=false"
        })
class AnnotationDrivenIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库，避免其他上下文的 Relay 认领本类留下的记录。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "anno");
    }

    private static final String QUEUE = "it.anno.queue";
    private static final String CONSUMER = "it-anno-consumer";

    /** 注解式事件载荷：事件类型与路由全部来自类上的 @OutboxEvent。 */
    @OutboxEvent(eventType = "it.anno.order.created", exchange = "it.anno.exchange")
    record AnnotatedOrderPayload(long orderId) { }

    /** 注解式 Handler：不写 eventType()/payloadType()，只实现业务方法。 */
    @OutboxHandler(event = AnnotatedOrderPayload.class, queue = QUEUE, consumerName = CONSUMER)
    static class AnnotatedOrderHandler extends AnnotatedOutboxHandler<AnnotatedOrderPayload> {
        static final Set<Long> RECEIVED = ConcurrentHashMap.newKeySet();

        @Override
        public void handle(EventContext<AnnotatedOrderPayload> context) {
            RECEIVED.add(context.getPayload().orderId());
        }
    }

    @OutboxEvent(eventType = "it.anno.disabled", exchange = "it.anno.disabled.exchange")
    record DisabledPayload(long id) { }

    @OutboxHandler(event = DisabledPayload.class, queue = "it.anno.disabled.queue",
            consumerName = "it-anno-disabled", retry = @org.outboxpro.core.annotation.RetryPolicySpec(enabled = false))
    static class DisabledHandler extends AnnotatedOutboxHandler<DisabledPayload> {
        static final java.util.concurrent.atomic.AtomicInteger INVOCATIONS = new java.util.concurrent.atomic.AtomicInteger();
        @Override public void handle(EventContext<DisabledPayload> context) {
            INVOCATIONS.incrementAndGet();
            throw new IllegalStateException("ordinary retryable failure");
        }
    }

    @Autowired
    org.springframework.jdbc.core.JdbcTemplate jdbc;

    /** 仅设置 enabled=false 时，普通异常也只能消费一次并直接记入死信台账。 */
    @Test
    void disabledOnlyRetrySkipsGlobalRetryAndGoesToDeadLetter() {
        DisabledHandler.INVOCATIONS.set(0);
        var envelope = publisher.publish(DisabledPayload.class, new DisabledPayload(9201));
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var rows = jdbc.queryForList("SELECT reason_code, attempt_count FROM outboxpro_dead_letter "
                    + "WHERE event_id = ? AND consumer_name = ?", envelope.getEventId(), "it-anno-disabled");
            assertThat(rows).singleElement().satisfies(row -> {
                assertThat(row.get("reason_code")).isEqualTo("HANDLER_FAILURE");
                assertThat(row.get("attempt_count")).isEqualTo(1);
            });
        });
        Awaitility.await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4))
                .untilAsserted(() -> assertThat(DisabledHandler.INVOCATIONS.get()).isEqualTo(1));
    }

    @Autowired
    OutboxProPublisher publisher;

    @Autowired
    EventRegistry eventRegistry;

    @Autowired
    AnnotatedOrderHandler annotatedHandler;

    @Autowired
    List<OutboxProSubscription> subscriptions;

    /** 测试专用配置：注册注解式 Handler，并以一致的 Builder 定义验证共存。 */
    @Configuration
    static class Config {

        @Bean
        DisabledHandler disabledHandler() { return new DisabledHandler(); }

        /** 同一事件的 Builder 与注解声明在真实 Spring 上下文中共存。 */
        @Bean
        org.outboxpro.core.event.EventDefinition<AnnotatedOrderPayload> annotatedDefinition() {
            return org.outboxpro.core.event.EventDefinition.<AnnotatedOrderPayload>builder()
                    .eventType("it.anno.order.created").payloadType(AnnotatedOrderPayload.class)
                    .route("it.anno.exchange", "it.anno.order.created").build();
        }

        @Bean
        AnnotatedOrderHandler annotatedOrderHandler() {
            return new AnnotatedOrderHandler();
        }
    }

    /** 注解式 Handler 生成的定义与订阅能支撑完整的发布消费链路。 */
    @Test
    void annotatedHandlerDrivesFullPipeline() {
        // 事件定义已由注解自动注册，且路由与注解声明一致。
        var definition = eventRegistry.find("it.anno.order.created");
        assertThat(definition).as("注解应已自动注册事件定义").isNotNull();
        assertThat(definition.getPayloadType()).isEqualTo(AnnotatedOrderPayload.class);
        assertThat(definition.getRoute().exchange()).isEqualTo("it.anno.exchange");
        assertThat(definition.getRoute().routingKey()).isEqualTo("it.anno.order.created");

        // 订阅已由注解自动注册（手工注册的单例可按类型发现）。
        assertThat(subscriptions)
                .anySatisfy(subscription -> {
                    assertThat(subscription.getQueue()).isEqualTo(QUEUE);
                    assertThat(subscription.getConsumerName()).isEqualTo(CONSUMER);
                    assertThat(subscription.getBindings()).singleElement().satisfies(binding -> {
                        assertThat(binding.eventType()).isEqualTo("it.anno.order.created");
                        assertThat(binding.consumeMode().name()).isEqualTo("RELIABLE");
                    });
                });

        // 类型安全发布：不写 eventType 字符串，按载荷类型反查。
        publisher.publish(AnnotatedOrderPayload.class, new AnnotatedOrderPayload(9101L));

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(AnnotatedOrderHandler.RECEIVED).contains(9101L));
    }

    /** 类型安全发布支持 extensions，且消费端可见。 */
    @Test
    void typedPublishCarriesExtensions() {
        long orderId = 9102L;

        publisher.publish(AnnotatedOrderPayload.class, new AnnotatedOrderPayload(orderId),
                Map.of("tenantId", "tenant-anno"));

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(AnnotatedOrderHandler.RECEIVED).contains(orderId));
    }

    /** 未注册的载荷类型触发快速失败；多义载荷类型提示改用 eventType 重载。 */
    @Test
    void typedPublishFailsFastOnAmbiguity() {
        assertThat(eventRegistry.requireByPayloadType(AnnotatedOrderPayload.class).getEventType())
                .isEqualTo("it.anno.order.created");
        // OrderCreatedPayload 从未在本上下文注册为任何事件。
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                        eventRegistry.requireByPayloadType(OrderCreatedPayload.class))
                .isInstanceOf(EventConfigurationException.class)
                .hasMessageContaining("No event registered");
    }
}
