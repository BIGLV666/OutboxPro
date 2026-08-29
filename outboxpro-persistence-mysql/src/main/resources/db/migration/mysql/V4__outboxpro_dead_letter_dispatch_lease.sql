-- 通过 information_schema + 动态 SQL 保证脚本可被迁移工具重复执行。
SET @outboxpro_dispatch_owner_ddl = (
    SELECT IF(
        EXISTS(
            SELECT 1 FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'outboxpro_dead_letter'
              AND COLUMN_NAME = 'dispatch_owner'
        ),
        'SELECT 1',
        'ALTER TABLE outboxpro_dead_letter ADD COLUMN dispatch_owner VARCHAR(100) NULL AFTER status'
    )
);
PREPARE outboxpro_dispatch_owner_stmt FROM @outboxpro_dispatch_owner_ddl;
EXECUTE outboxpro_dispatch_owner_stmt;
DEALLOCATE PREPARE outboxpro_dispatch_owner_stmt;

SET @outboxpro_dispatch_until_ddl = (
    SELECT IF(
        EXISTS(
            SELECT 1 FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'outboxpro_dead_letter'
              AND COLUMN_NAME = 'dispatch_until'
        ),
        'SELECT 1',
        'ALTER TABLE outboxpro_dead_letter ADD COLUMN dispatch_until DATETIME(6) NULL AFTER dispatch_owner'
    )
);
PREPARE outboxpro_dispatch_until_stmt FROM @outboxpro_dispatch_until_ddl;
EXECUTE outboxpro_dispatch_until_stmt;
DEALLOCATE PREPARE outboxpro_dispatch_until_stmt;
