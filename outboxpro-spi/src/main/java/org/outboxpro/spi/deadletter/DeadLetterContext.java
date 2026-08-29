package org.outboxpro.spi.deadletter;

import java.time.Instant;

/**
 * 传递给死信策略的不可变上下文。
 *
 * @param eventId 原始事件 ID
 * @param eventType 原始事件类型；畸形消息无法获得时为 {@code null}
 * @param consumerName 发生死信的消费者名称
 * @param queue 原消费队列
 * @param originalExchange 人工重放时使用的原业务交换机
 * @param originalRoutingKey 人工重放时使用的原业务路由键
 * @param payloadJson 原始消息 JSON；由框架保存以支持可选台账和重放
 * @param attempt 当前消费尝试次数
 * @param reason 框架确定的不可变死信原因
 * @param occurredAt 死信发生时间
 */
public record DeadLetterContext(String eventId, String eventType, String consumerName, String queue,
                                String originalExchange, String originalRoutingKey, String payloadJson,
                                int attempt, DeadLetterReason reason, Instant occurredAt) {
    /**
     * 创建死信上下文并校验保证可靠处置所需的关键字段。
     *
     * @throws IllegalArgumentException 当事件 ID、消费者、路由或原因缺失时抛出
     */
    public DeadLetterContext {
        require(eventId, "eventId");
        require(consumerName, "consumerName");
        require(queue, "queue");
        require(originalExchange, "originalExchange");
        require(originalRoutingKey, "originalRoutingKey");
        if (payloadJson == null) {
            throw new IllegalArgumentException("payloadJson must not be null");
        }
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be at least 1");
        }
        if (reason == null || occurredAt == null) {
            throw new IllegalArgumentException("reason and occurredAt must not be null");
        }
    }

    /** 校验字符串字段非空且不全为空白。 */
    private static void require(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}

