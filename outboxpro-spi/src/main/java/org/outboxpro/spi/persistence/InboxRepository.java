package org.outboxpro.spi.persistence;

/**
 * Inbox 持久化扩展点，负责消费开始、成功、失败和忽略状态。
 * 实现必须以 consumerName + eventId 唯一约束作为幂等基础。
 */
public interface InboxRepository {
    /**
     * 尝试开始一次消费。
     * @param consumerName 当前订阅的消费者名称
     * @param record 待处理事件信息
     * @return 首次开始或允许重试返回 true；已经 SUCCESS 返回 false
     */
    boolean tryStart(String consumerName, InboxRecord record);
    /** @param consumerName 消费者名称 @param eventId 事件 ID。 */
    void markSuccess(String consumerName, String eventId);
    /** @param consumerName 消费者名称 @param eventId 事件 ID @param errorMessage 脱敏后的错误信息。 */
    void markFailed(String consumerName, String eventId, String errorMessage);
    /** @param consumerName 消费者名称 @param eventId 事件 ID @param errorMessage 脱敏后的错误信息。 */
    void markIgnored(String consumerName, String eventId, String errorMessage);

    /**
     * 清理指定时间之前处理成功（SUCCESS）的 Inbox 记录，供内置保留策略使用。
     * 仅允许清理 SUCCESS 状态：FAILED / IGNORED / RECEIVED 记录是排障与重开依据，不属于保留范围。
     * 实现应限制单次删除行数，避免长事务锁表。
     *
     * @param cutoff 清理该时间之前处理成功的记录
     * @param limit 单次最多删除行数
     * @return 实际删除行数；实现不支持时返回 0
     */
    default int purgeSuccessBefore(java.time.Instant cutoff, int limit) {
        return 0;
    }
}
