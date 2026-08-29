package org.outboxpro.transport.rabbit;

/**
 * 表示 RabbitMQ 消息无法解析为合法 OutboxPro Envelope 的内部异常。
 *
 * <p>该异常只用于驱动固定的 {@code MALFORMED_MESSAGE} 死信分类，不允许被用户策略重写。</p>
 */
final class MalformedMessageException extends RuntimeException {
    /**
     * 创建消息格式异常。
     *
     * @param message 脱敏后的格式错误描述
     * @param cause 原始解析异常
     */
    MalformedMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
