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

    /** @param jdbc Spring JDBC 模板。 */
    public JdbcInboxRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 尝试创建或重新打开 Inbox 记录。
     * 已经 SUCCESS 的消息返回 false；FAILED 和 IGNORED 可被重试流程重新置为 RECEIVED。
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
            int updated = jdbc.update("""
                    UPDATE outboxpro_inbox
                    SET status = 'RECEIVED',
                        retry_count = retry_count + 1,
                        last_error = NULL,
                        updated_time = NOW(),
                        version = version + 1
                    WHERE consumer_name = ?
                      AND event_id = ?
                      AND status IN ('FAILED', 'IGNORED')
                    """, consumerName, record.eventId());
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
}
