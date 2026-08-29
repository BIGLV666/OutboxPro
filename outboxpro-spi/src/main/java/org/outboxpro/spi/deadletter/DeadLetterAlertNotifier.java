package org.outboxpro.spi.deadletter;

/**
 * 用户可替换的 DLQ 堆积告警通知器。
 *
 * <p>通知器为旁路能力；其异常应由调用方隔离，不能阻塞消息消费、死信落库或重放。</p>
 */
public interface DeadLetterAlertNotifier {
    /**
     * 待人工处理的死信数量首次达到告警阈值时调用。
     *
     * @param pendingCount 当前精确的待重放死信数量
     * @param threshold 触发告警的阈值
     */
    void onHighWatermark(long pendingCount, long threshold);

    /**
     * 死信数量回落至恢复阈值以下且经过冷却时长时调用。
     *
     * @param pendingCount 当前精确的待重放死信数量
     * @param recoveryThreshold 恢复阈值
     */
    void onRecovered(long pendingCount, long recoveryThreshold);

    /**
     * 框架模式下的旁路通知：死信已可靠写入 RabbitMQ DLQ，用户可归档、发送工单等。
     *
     * <p>默认空实现，用户可选择实现。</p>
     *
     * @param context 不可变死信上下文
     */
    default void notify(DeadLetterContext context) {
        // 默认空实现
    }
}
