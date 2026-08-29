package org.outboxpro.core.context;

import org.outboxpro.core.envelope.EventEnvelope;

/**
 * Handler 接收的事件上下文，暴露不可变事件信封、类型化载荷和链路标识。
 * 业务代码不应通过此对象直接操作 Inbox、ACK 或 RabbitMQ。
 *
 * @param <T> 载荷类型
 */
public final class EventContext<T> {
    private final EventEnvelope<T> envelope;

    /**
     * 创建事件上下文。
     *
     * @param envelope 当前消费的不可变事件信封
     */
    public EventContext(EventEnvelope<T> envelope) { this.envelope = envelope; }

    /** @return 当前事件信封。 */
    public EventEnvelope<T> getEnvelope() { return envelope; }
    /** @return 类型化业务载荷。 */
    public T getPayload() { return envelope.getPayload(); }
    /** @return 当前事件唯一 ID。 */
    public String getEventId() { return envelope.getEventId(); }
    /** @return 当前消息的 Trace ID，可能为空。 */
    public String getTraceId() { return envelope.getTraceId(); }
}
