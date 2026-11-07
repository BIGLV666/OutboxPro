package org.outboxpro.core.metrics;

import java.time.Duration;

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

    /**
     * 单条消息从写出到收到 Publisher Confirm 的耗时。
     * 发布器实现按记录上报；未接入指标的实现保持空实现。
     *
     * @param eventType 事件类型
     * @param producer 生产者名称
     * @param duration 发布耗时
     */
    default void publishDuration(String eventType, String producer, Duration duration) { }

    /** Publisher Confirm 等待超时（与 Broker NACK、连接失败区分，便于单独告警）。 */
    default void publishConfirmTimeout(String eventType, String producer) { }

    /**
     * Relay 单轮执行耗时，用于观察认领 + 发布 + 状态回写整体开销。
     *
     * @param duration 单轮耗时
     * @param claimed 本轮是否认领到消息；false 表示空闲轮
     */
    default void relayCycle(Duration duration, boolean claimed) { }

    /** Relay 单轮认领的消息数量。 */
    default void relayClaimed(int count) { }

    /**
     * Relay 状态迁移时发现认领租约已丢失（发布成功或失败，但记录已被其他实例复位重认领）。
     * 这是 claimTimeout 配置过短或实例长时间停顿的强信号，应当告警。
     */
    default void relayLeaseLost(String eventType, String producer) { }

    /** 数据库消息日志 Sink 因队列满而丢弃日志记录。 */
    default void logSinkDropped(int count) { }

    /** 数据库消息日志 Sink 批量写入失败，整批日志被丢弃。 */
    default void logSinkFlushFailed(int count) { }

    /** 消费端开始处理一条已绑定事件的消息。 */
    default void consumeStarted(String eventType, String consumer, String queue) { }

    /** 消费端成功处理（Reliable 事务提交或 Best Effort 记 IGNORED 后 ACK）。 */
    default void consumeSucceeded(String eventType, String consumer, String queue) { }

    /** 消费端处理抛出异常。 */
    default void consumeFailed(String eventType, String consumer, String queue) { }

    /**
     * 消费端单条消息的处理耗时（含 Handler、Inbox 与本地事务）。
     *
     * @param eventType 事件类型
     * @param consumer 消费方名称
     * @param queue 队列名称
     * @param duration 处理耗时
     */
    default void consumeDuration(String eventType, String consumer, String queue, Duration duration) { }

    /** 消费端失败消息已可靠写入 Retry Queue。 */
    default void consumeRetried(String eventType, String consumer, String queue) { }

    /** 消费端失败消息已进入死信流程（重试耗尽 / 不可重试 / 未绑定事件 / 报文损坏）。 */
    default void consumeDead(String eventType, String consumer, String queue) { }

    /** 消费端一条消息已成功进入死信流程，携带框架稳定原因码。 */
    default void consumeDead(String eventType, String consumer, String queue, String reasonCode) {
        consumeDead(eventType, consumer, queue);
    }

    /** Inbox 判定重复投递并跳过业务执行。 */
    default void inboxDuplicate(String eventType, String consumer) { }
}
