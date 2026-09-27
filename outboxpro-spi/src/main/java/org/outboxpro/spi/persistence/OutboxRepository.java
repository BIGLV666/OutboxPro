package org.outboxpro.spi.persistence;

import java.time.Instant;
import java.util.List;

/**
 * Outbox 持久化扩展点，认领操作必须具备多实例并发安全性。
 * 实现应让 insert、状态更新参与调用方当前本地数据库事务。
 */
public interface OutboxRepository {
    /** @param record 待写入的 Outbox 记录。 */
    void insert(OutboxRecord record);
    /**
     * 批量认领可投递记录。
     * @param owner 当前 Relay 实例标识
     * @param batchSize 最大认领数量
     * @param now 当前时间
     * @param claimUntil 认领租约到期时间
     * @return 成功被当前实例认领的记录
     */
    List<OutboxRecord> claimBatch(String owner, int batchSize, Instant now, Instant claimUntil);
    /** @param id Outbox 主键 @param owner 当前实例标识 @param sentAt 发布确认时间。 */
    void markSent(long id, String owner, Instant sentAt);
    /**
     * 标记为等待重试。
     * @param id Outbox 主键 @param owner 当前实例标识 @param attempt 当前尝试次数
     * @param nextRetryAt 下次重试时间 @param errorType 错误类型 @param errorMessage 脱敏后的错误信息
     */
    void markRetryWaiting(long id, String owner, int attempt, Instant nextRetryAt, String errorType, String errorMessage);
    /** @param id Outbox 主键 @param owner 当前实例标识 @param attempt 最终尝试次数 @param errorType 错误类型 @param errorMessage 脱敏后的错误信息。 */
    void markDead(long id, String owner, int attempt, String errorType, String errorMessage);
    /** @param now 当前时间 @return 被恢复为 PENDING 的记录数量。 */
    int recoverExpiredClaims(Instant now);

    /**
     * 在租约校验下把已确认消息迁移到 SENT，并报告状态迁移是否真正生效。
     *
     * <p>认领租约可能被其他实例的过期恢复复位（此时 {@code claim_owner} 已被清空），
     * 调用方需要通过返回值感知"发布成功但状态未迁移"的竞态，以便告警。
     * 默认实现调用 {@link #markSent(long, String, Instant)} 并返回 {@code true}，
     * 保持既有自定义实现的二进制兼容；能拿到影响行数的实现应覆写本方法。</p>
     *
     * @param id Outbox 主键
     * @param owner 当前实例标识
     * @param sentAt 发布确认时间
     * @return 状态确实迁移到 SENT 时返回 {@code true}；租约已丢失返回 {@code false}
     */
    default boolean transitionToSent(long id, String owner, Instant sentAt) {
        markSent(id, owner, sentAt);
        return true;
    }

    /**
     * 在租约校验下把发布失败的消息迁移到 RETRY_WAITING，语义同 {@link #transitionToSent(long, String, Instant)}。
     *
     * @param id Outbox 主键
     * @param owner 当前实例标识
     * @param attempt 当前尝试次数
     * @param nextRetryAt 下次重试时间
     * @param errorType 错误类型
     * @param errorMessage 脱敏后的错误信息
     * @return 状态确实迁移时返回 {@code true}；租约已丢失返回 {@code false}
     */
    default boolean transitionToRetryWaiting(long id, String owner, int attempt, Instant nextRetryAt,
                                             String errorType, String errorMessage) {
        markRetryWaiting(id, owner, attempt, nextRetryAt, errorType, errorMessage);
        return true;
    }

    /**
     * 在租约校验下把发布失败的消息迁移到 DEAD，语义同 {@link #transitionToSent(long, String, Instant)}。
     *
     * @param id Outbox 主键
     * @param owner 当前实例标识
     * @param attempt 最终尝试次数
     * @param errorType 错误类型
     * @param errorMessage 脱敏后的错误信息
     * @return 状态确实迁移时返回 {@code true}；租约已丢失返回 {@code false}
     */
    default boolean transitionToDead(long id, String owner, int attempt, String errorType, String errorMessage) {
        markDead(id, owner, attempt, errorType, errorMessage);
        return true;
    }

    /**
     * 清理指定时间之前已投递成功（SENT）的 Outbox 记录，供内置保留策略使用。
     * 实现应限制单次删除行数，避免长事务锁表。
     *
     * @param cutoff 清理该时间之前投递成功的记录
     * @param limit 单次最多删除行数
     * @return 实际删除行数；实现不支持时返回 0
     */
    default int purgeSentBefore(Instant cutoff, int limit) {
        return 0;
    }

    /**
     * 按条件分页检索 Outbox 消息，供运维查询使用。
     * 返回按 id 倒序排列，条件字段全部精确匹配。
     *
     * @param query 检索条件
     * @return 命中的消息记录；不支持检索的实现返回空列表
     */
    default List<OutboxRecord> findMessages(OutboxQuery query) {
        return List.of();
    }

    /**
     * 统计检索条件命中的消息总数，供运维分页展示。
     *
     * @param query 检索条件
     * @return 命中总数；不支持检索的实现返回 0
     */
    default long countMessages(OutboxQuery query) {
        return 0L;
    }

    /**
     * 按事件 ID 精确查询单条 Outbox 消息（含载荷），供运维排障使用。
     *
     * @param eventId 事件唯一 ID
     * @return 命中的消息记录；不存在或实现不支持时返回 {@code null}
     */
    default OutboxRecord findByEventId(String eventId) {
        return null;
    }

    /**
     * 把 DEAD 消息复位为 PENDING，交还给 Relay 重新投递（生产端人工重放）。
     * 条件更新保证并发调用与正常状态机安全：只有仍处于 DEAD 的记录会被复位，
     * 复位后重试次数清零、下次重试时间清空。
     *
     * @param eventId 事件唯一 ID
     * @return 是否成功复位；消息不存在、不是 DEAD 状态或实现不支持时返回 {@code false}
     */
    default boolean resetDeadForReplay(String eventId) {
        return false;
    }
}
