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

    /**
     * 类型安全发布：按载荷类型反查已注册的事件类型后写入 Outbox。
     * 避免业务代码手写 eventType 字符串；载荷类型必须与唯一的已注册事件定义匹配。
     *
     * <p>实现默认抛出 {@link UnsupportedOperationException}，由框架默认发布器提供真实行为。</p>
     *
     * @param payloadType 载荷 Java 类型，必须对应唯一的已注册事件定义
     * @param payload 事件业务载荷，可为 {@code null}
     * @param <T> 载荷类型
     * @return 已生成 eventId 的不可变事件信封
     * @throws UnsupportedOperationException 发布器实现不支持类型安全发布时抛出
     */
    default <T> EventEnvelope<T> publish(Class<T> payloadType, T payload) {
        throw new UnsupportedOperationException(
                "publish(Class, payload) is not supported by this OutboxProPublisher implementation");
    }

    /**
     * 类型安全发布并附带扩展元数据，语义同 {@link #publish(Class, Object)}。
     *
     * <p>实现默认抛出 {@link UnsupportedOperationException}，由框架默认发布器提供真实行为。</p>
     *
     * @param payloadType 载荷 Java 类型，必须对应唯一的已注册事件定义
     * @param payload 事件业务载荷，可为 {@code null}
     * @param extensions 非敏感的扩展元数据
     * @param <T> 载荷类型
     * @return 已生成 eventId 的不可变事件信封
     * @throws UnsupportedOperationException 发布器实现不支持类型安全发布时抛出
     */
    default <T> EventEnvelope<T> publish(Class<T> payloadType, T payload, Map<String, Object> extensions) {
        throw new UnsupportedOperationException(
                "publish(Class, payload, extensions) is not supported by this OutboxProPublisher implementation");
    }
}
