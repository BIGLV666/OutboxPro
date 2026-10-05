-- 为当前待重放死信的原因分布查询提供窄索引。
-- 使用 information_schema + 动态 SQL 保证脚本可被迁移工具重复执行。
SET @outboxpro_status_reason_index_ddl = (
    SELECT IF(
        EXISTS(
            SELECT 1 FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'outboxpro_dead_letter'
              AND INDEX_NAME = 'idx_dead_letter_status_reason'
        ),
        'SELECT 1',
        'ALTER TABLE outboxpro_dead_letter ADD INDEX idx_dead_letter_status_reason (status, reason_code)'
    )
);
PREPARE outboxpro_status_reason_index_stmt FROM @outboxpro_status_reason_index_ddl;
EXECUTE outboxpro_status_reason_index_stmt;
DEALLOCATE PREPARE outboxpro_status_reason_index_stmt;
