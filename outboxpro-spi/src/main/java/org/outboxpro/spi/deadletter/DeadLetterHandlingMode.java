package org.outboxpro.spi.deadletter;

/**
 * 死信处理模式。
 */
public enum DeadLetterHandlingMode {
    /**
     * 框架固定发布 RabbitMQ DLQ，用户策略做旁路通知。
     */
    FRAMEWORK,
    
    /**
     * 用户策略完全接管死信发布，框架只在用户返回 ACCEPTED 时 ACK 原消息。
     */
    CUSTOM
}
