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

    @Autowired
    OutboxProPublisher publisher;

    @Autowired
    EventRegistry eventRegistry;

    @Autowired
    AnnotatedOrderHandler annotatedHandler;

    @Autowired
    List<OutboxProSubscription> subscriptions;

    /** 测试专用配置：只注册注解式 Handler 实例，不声明任何定义/订阅 Bean。 */
    @Configuration
    static class Config {

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
