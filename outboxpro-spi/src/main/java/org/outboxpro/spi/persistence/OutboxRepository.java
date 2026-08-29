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
}
