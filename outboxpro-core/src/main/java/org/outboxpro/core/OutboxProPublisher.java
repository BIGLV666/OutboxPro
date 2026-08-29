package org.outboxpro.core;

import org.outboxpro.core.envelope.EventEnvelope;

import java.util.Map;

/**
 * 业务事务中使用的事件发布契约。
 * 实现必须把消息写入调用方当前数据库事务中的 Outbox，而不是直接访问 RabbitMQ。
 */
public interface OutboxProPublisher {
    /**
     * 在当前业务事务中创建事件信封并写入 Outbox。
     *
     * @param eventType 已注册的事件类型
     * @param payload 事件业务载荷，可为 {@code null}
     * @param <T> 载荷类型
     * @return 已生成 eventId 的不可变事件信封
     * @throws IllegalArgumentException 载荷类型与事件定义不匹配时抛出
     * @throws RuntimeException Outbox 写入失败时抛出并触发当前事务回滚
     */
    <T> EventEnvelope<T> publish(String eventType, T payload);

    /**
     * 在当前业务事务中写入带扩展元数据的事件。
     *
     * @param eventType 已注册的事件类型
     * @param payload 事件业务载荷，可为 {@code null}
     * @param extensions 非敏感的扩展元数据
     * @param <T> 载荷类型
     * @return 已生成 eventId 的不可变事件信封
     */
    <T> EventEnvelope<T> publish(String eventType, T payload, Map<String, Object> extensions);
}
