package org.outboxpro.core.event;

/** 事件发往 RabbitMQ 的交换机和路由键。 */
/**
 * 事件传输路由，描述目标交换机和路由键。
 */
public record EventRoute(String exchange, String routingKey) {
    public EventRoute {
        if (exchange == null || exchange.isBlank()) throw new IllegalArgumentException("exchange must not be blank");
        if (routingKey == null || routingKey.isBlank()) throw new IllegalArgumentException("routingKey must not be blank");
    }
}



