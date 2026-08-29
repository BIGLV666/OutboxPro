package org.outboxpro.spi.deadletter;

/**
 * 用户定义死信策略返回的可靠接收结果。
 *
 * <p>框架只在 {@link #ACCEPTED} 时确认原 RabbitMQ 消息；其余结果会触发 NACK 和重新入队。</p>
 */
public enum DeadLetterHandlingResult {
    /** 自定义策略已经把死信副本可靠交给其目标系统，原消息可被 ACK。 */
    ACCEPTED,
    /** 自定义策略未能可靠接收死信副本，原消息必须重新入队。 */
    REJECTED
}
