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
}
