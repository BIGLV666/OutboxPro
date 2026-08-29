package org.outboxpro.core.metrics;

/**
 * OutboxPro 指标门面：核心与适配器模块通过它上报运行指标，不依赖任何指标实现。
 *
 * <p>所有方法默认空实现，未装配指标时零开销。实现方（如 Micrometer 适配器）
 * 必须保持标签低基数：只使用 event_type / producer / consumer / queue 等业务枚举值，
 * 绝不把 eventId、payload 或用户输入放进标签——无法确认归属的事件统一记为 unknown。</p>
 */
public interface OutboxMetrics {

    /** 专用 no-op 实例，供未装配指标实现时使用。 */
    OutboxMetrics NOOP = new OutboxMetrics() { };

    /** 生产端一次 MQ 发布尝试（Relay 认领后调用）。 */
    default void publishAttempt(String eventType, String producer) { }

    /** 生产端发布获得 Publisher Confirm。 */
    default void publishSuccess(String eventType, String producer) { }

    /** 生产端发布失败（将进入重试或 DEAD）。 */
    default void publishFailure(String eventType, String producer) { }

    /** Relay 单轮认领的消息数量。 */
    default void relayClaimed(int count) { }

    /** 消费端开始处理一条已绑定事件的消息。 */
    default void consumeStarted(String eventType, String consumer, String queue) { }

    /** 消费端成功处理（Reliable 事务提交或 Best Effort 记 IGNORED 后 ACK）。 */
    default void consumeSucceeded(String eventType, String consumer, String queue) { }

    /** 消费端处理抛出异常。 */
    default void consumeFailed(String eventType, String consumer, String queue) { }

    /** 消费端失败消息已可靠写入 Retry Queue。 */
    default void consumeRetried(String eventType, String consumer, String queue) { }

    /** 消费端失败消息已进入死信流程（重试耗尽 / 不可重试 / 未绑定事件 / 报文损坏）。 */
    default void consumeDead(String eventType, String consumer, String queue) { }

    /** Inbox 判定重复投递并跳过业务执行。 */
    default void inboxDuplicate(String eventType, String consumer) { }
}
