package org.outboxpro.persistence.mysql;

import org.outboxpro.spi.persistence.InboxRecord;
import org.outboxpro.spi.persistence.InboxRepository;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;

/**
 * 基于 Spring JDBC 的 MySQL Inbox Repository，通过唯一键阻止重复业务执行。
 * 唯一约束为 {@code consumer_name + event_id}，不是单靠乐观锁实现幂等。
 *
 * <p>类不能声明为 final：方法上的 {@code @Transactional} 需要 Spring 生成 CGLIB 子类代理。</p>
 */
public class JdbcInboxRepository implements InboxRepository {
    private final JdbcTemplate jdbc;
    private final java.time.Duration receivedStaleTimeout;

    /** @param jdbc Spring JDBC 模板。 */
    public JdbcInboxRepository(JdbcTemplate jdbc) {
        this(jdbc, java.time.Duration.ofMinutes(10));
    }

    /**
     * @param jdbc Spring JDBC 模板
     * @param receivedStaleTimeout RECEIVED 孤儿记录的重开超时；必须大于最坏 Handler 执行时长，
     *                             否则并发重复投递会在正常执行期间互相重开造成双重执行
     */
    public JdbcInboxRepository(JdbcTemplate jdbc, java.time.Duration receivedStaleTimeout) {
        if (receivedStaleTimeout == null || receivedStaleTimeout.isNegative() || receivedStaleTimeout.isZero()) {
            throw new IllegalArgumentException("receivedStaleTimeout must be positive");
        }
        this.jdbc = jdbc;
        this.receivedStaleTimeout = receivedStaleTimeout;
    }

    /**
     * 尝试创建或重新打开 Inbox 记录。
     * 已经 SUCCESS 的消息返回 false；FAILED、IGNORED 以及超过孤儿超时的 RECEIVED
     * 可被重投递重新置为 RECEIVED。
     */
    @Override
    @Transactional
    public boolean tryStart(String consumerName, InboxRecord record) {
        try {
            jdbc.update("""
                    INSERT INTO outboxpro_inbox (
                        consumer_name, event_id, event_type, status,
                        retry_count, received_time, updated_time, version
                    ) VALUES (?, ?, ?, 'RECEIVED', ?, ?, NOW(), 0)
                    """, consumerName, record.eventId(), record.eventType(),
                    record.retryCount(), Timestamp.from(record.receivedTime()));
            return true;
        } catch (DuplicateKeyException duplicate) {
            // 唯一键冲突不是错误：先判断是否已经成功，成功消息不能再次执行业务逻辑。
            Integer successCount = jdbc.queryForObject("""
                    SELECT COUNT(*)
                    FROM outboxpro_inbox
                    WHERE consumer_name = ?
                      AND event_id = ?
                      AND status = 'SUCCESS'
                    """, Integer.class, consumerName, record.eventId());
            if (successCount != null && successCount > 0) {
                return false;
            }

            // 失败或忽略的记录可以被新的投递重新打开，同时累加 retry_count。
            // RECEIVED 孤儿记录（进程在 Best Effort 执行中途崩溃后留下的行）超过孤儿超时后同样允许重开，
            // 否则该消息既未执行成功也永远无法重放；超时下限由校验器保证大于并发执行窗口。
            java.sql.Timestamp staleBefore = java.sql.Timestamp.from(
                    java.time.Instant.now().minus(receivedStaleTimeout));
            int updated = jdbc.update("""
                    UPDATE outboxpro_inbox
                    SET status = 'RECEIVED',
                        retry_count = retry_count + 1,
                        last_error = NULL,
                        updated_time = NOW(),
                        version = version + 1
                    WHERE consumer_name = ?
                      AND event_id = ?
                      AND (status IN ('FAILED', 'IGNORED')
                           OR (status = 'RECEIVED' AND updated_time < ?))
                    """, consumerName, record.eventId(), staleBefore);
            return updated == 1;
        }
    }

    /** 将当前消费者的 Inbox 记录标记为 SUCCESS。 */
    @Override
    public void markSuccess(String consumerName, String eventId) {
        jdbc.update("""
                UPDATE outboxpro_inbox
                SET status = 'SUCCESS',
                    processed_time = NOW(),
                    updated_time = NOW(),
                    version = version + 1
                WHERE consumer_name = ?
                  AND event_id = ?
                """, consumerName, eventId);
    }

    /** 将当前消费者的 Inbox 记录标记为 FAILED。 */
    @Override
    public void markFailed(String consumerName, String eventId, String errorMessage) {
        updateFailureStatus("FAILED", consumerName, eventId, errorMessage);
    }

    /** 将 Best Effort 的失败记录标记为 IGNORED。 */
    @Override
    public void markIgnored(String consumerName, String eventId, String errorMessage) {
        updateFailureStatus("IGNORED", consumerName, eventId, errorMessage);
    }

    private void updateFailureStatus(String status, String consumerName, String eventId, String errorMessage) {
        jdbc.update("""
                UPDATE outboxpro_inbox
                SET status = ?,
                    last_error = ?,
                    updated_time = NOW(),
                    version = version + 1
                WHERE consumer_name = ?
                  AND event_id = ?
                """, status, truncate(errorMessage), consumerName, eventId);
    }

    private String truncate(String value) {
        return value == null ? null : value.substring(0, Math.min(value.length(), 2000));
    }

    /**
     * 分批清理处理成功的记录：只清理 SUCCESS 终态，按主键序限量删除，避免长事务锁表。
     */
    @Override
    public int purgeSuccessBefore(java.time.Instant cutoff, int limit) {
        return jdbc.update("""
                DELETE FROM outboxpro_inbox
                WHERE status = 'SUCCESS' AND processed_time IS NOT NULL AND processed_time < ?
                ORDER BY id
                LIMIT ?
                """, Timestamp.from(cutoff), limit);
    }
}
