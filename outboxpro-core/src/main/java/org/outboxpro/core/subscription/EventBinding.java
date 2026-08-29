package org.outboxpro.core.subscription;

import org.outboxpro.core.retry.RetryPolicy;

/** 队列上的单个事件绑定，包含事件类型、路由键、载荷类型、消费模式和重试策略。 */
public record EventBinding(String eventType, String routingKey, Class<?> payloadType, ConsumeMode consumeMode, RetryPolicy retryPolicy) {
    public EventBinding {
        if (eventType == null || eventType.isBlank()) throw new IllegalArgumentException("eventType must not be blank");
        if (routingKey == null || routingKey.isBlank()) throw new IllegalArgumentException("routingKey must not be blank");
        if (payloadType == null) throw new IllegalArgumentException("payloadType must not be null");
        if (consumeMode == null) throw new IllegalArgumentException("consumeMode must not be null");
        if (retryPolicy == null) retryPolicy = RetryPolicy.defaults();
    }

    /**
     * 创建 Reliable 绑定。
     * @param eventType 事件类型
     * @param routingKey RabbitMQ 路由键
     * @param payloadType 载荷类型
     * @return Reliable 事件绑定
     */
    public static EventBinding reliable(String eventType, String routingKey, Class<?> payloadType) {
        return new EventBinding(eventType, routingKey, payloadType, ConsumeMode.RELIABLE, RetryPolicy.defaults());
    }

    /**
     * 创建 Best Effort 绑定。
     * @param eventType 事件类型
     * @param routingKey RabbitMQ 路由键
     * @param payloadType 载荷类型
     * @return Best Effort 事件绑定
     */
    public static EventBinding bestEffort(String eventType, String routingKey, Class<?> payloadType) {
        return new EventBinding(eventType, routingKey, payloadType, ConsumeMode.BEST_EFFORT, RetryPolicy.defaults());
    }
}
