package org.outboxpro.example.order.config;

import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.outboxpro.example.order.event.OrderCreated;
import org.outboxpro.spi.deadletter.DeadLetterContext;
import org.outboxpro.spi.deadletter.DlqReplayAuthorizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OutboxPro 声明式配置：业务只需声明事件定义和队列订阅，其余交给框架。
 */
@Configuration
public class OutboxProConfig {

    public static final String ORDER_CREATED = "order.created";
    public static final String ORDER_EXCHANGE = "order.exchange";

    /** 事件定义：声明事件类型、载荷类型与 RabbitMQ 路由。 */
    @Bean
    public EventDefinition<OrderCreated> orderCreatedDefinition() {
        return EventDefinition.<OrderCreated>builder()
                .eventType(ORDER_CREATED)
                .schemaVersion("v1")
                .payloadType(OrderCreated.class)
                .route(ORDER_EXCHANGE, ORDER_CREATED)
                .build();
    }

    /** 库存订阅：Reliable 模式，消费失败会重试并最终进入 DLQ。 */
    @Bean
    public OutboxProSubscription inventorySubscription() {
        return OutboxProSubscription.builder()
                .name("inventory-service")
                .exchange(ORDER_EXCHANGE)
                .queue("order-demo.inventory.queue")
                .bindings(EventBinding.reliable(ORDER_CREATED, ORDER_CREATED, OrderCreated.class))
                .build();
    }

    /** 通知订阅：Best Effort 模式，消费失败只记 IGNORED 并 ACK，不阻塞队列。 */
    @Bean
    public OutboxProSubscription notificationSubscription() {
        return OutboxProSubscription.builder()
                .name("notification-service")
                .exchange(ORDER_EXCHANGE)
                .queue("order-demo.notification.queue")
                .bindings(EventBinding.bestEffort(ORDER_CREATED, ORDER_CREATED, OrderCreated.class))
                .build();
    }

    /**
     * 重放授权器：示例为演示直接放行所有操作员。
     * 生产环境必须接入真实权限体系（如 Spring Security 的当前用户校验），绝不放行匿名请求。
     */
    @Bean
    public DlqReplayAuthorizer demoReplayAuthorizer() {
        return (eventId, operator) -> {
            // 演示环境：记录即可，全部放行
            return;
        };
    }
}
