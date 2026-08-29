package org.outboxpro.core.handler;

import org.outboxpro.core.context.EventContext;

/**
 * 业务事件处理器契约。
 * Handler 只负责业务逻辑，不负责 Inbox、ACK、重试、日志或 Envelope 解析。
 *
 * @param <T> 载荷类型
 */
public interface OutboxProHandler<T> {
    /** @return Handler 处理的事件类型，必须与订阅绑定一致。 */
    String eventType();
    /** @return Handler 期望的载荷类型。 */
    Class<T> payloadType();
    /**
     * @return 本 Handler 归属的消费者名称（对应订阅的 consumerName）。
     *         同一事件类型只有一个 Handler 时返回 null 即可；
     *         多个消费方（多条订阅）消费同一事件类型时，每个 Handler 必须声明
     *         不同的 consumerName，框架按此把消息路由给对应订阅的 Handler。
     */
    default String consumerName() { return null; }
    /**
     * 执行业务事件处理逻辑。
     * @param context 当前事件上下文
     * @throws RuntimeException 业务失败时抛出，Reliable 模式会触发重试或死信
     */
    void handle(EventContext<T> context);
}
