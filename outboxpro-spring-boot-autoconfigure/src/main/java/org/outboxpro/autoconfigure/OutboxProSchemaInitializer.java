package org.outboxpro.autoconfigure;

import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;

/**
 * MySQL Schema 初始化器，按全局和死信台账配置执行幂等 DDL。
 *
 * <p>全局 {@code outboxpro.schema-initialize} 是总开关；死信台账的 V2 DDL 还受
 * {@code outboxpro.dlq.ledger.schema-initialize} 单独控制。</p>
 */
public final class OutboxProSchemaInitializer {
    private final DataSource dataSource;
    private final OutboxProProperties properties;

    /**
     * 创建 Schema 初始化器。
     *
     * @param dataSource 目标数据源
     * @param properties OutboxPro 配置
     */
    public OutboxProSchemaInitializer(DataSource dataSource, OutboxProProperties properties) {
        this.dataSource = dataSource;
        this.properties = properties;
    }

    /**
     * 执行基础表和可选死信表的启动初始化。
     *
     * <p>基础 V1 表和死信 V2 表分别执行，避免 ledger.schema-initialize=false 时误创建死信表。</p>
     */
    public void initialize() {
        // 全局关闭时不执行任何框架 DDL，交由 Flyway、Liquibase 或 DBA 管理。
        if (!properties.isSchemaInitialize()) {
            return;
        }

        execute("db/migration/mysql/V1__outboxpro.sql");

        // 只有台账开启且允许自动建表时才创建死信明细和计数桶表。
        if (properties.getDlq().getLedger().isEnabled()
                && properties.getDlq().getLedger().isSchemaInitialize()) {
            execute("db/migration/mysql/V2__outboxpro_dead_letter.sql");
            execute("db/migration/mysql/V3__outboxpro_dead_letter_raw_payload.sql");
            ensureDeadLetterDispatchLeaseColumns();
        }
    }


    /**
     * 按列检查并补齐死信分派租约字段。
     *
     * <p>不直接重复执行普通 {@code ALTER TABLE ADD COLUMN}，避免应用第二次启动因重复列失败；
     * 多实例同时启动发生竞态时，会在 DDL 异常后重新检查列是否已由其他实例创建。</p>
     */
    private void ensureDeadLetterDispatchLeaseColumns() {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        ensureColumn(jdbc, "dispatch_owner", """
                ALTER TABLE outboxpro_dead_letter
                ADD COLUMN dispatch_owner VARCHAR(100) NULL AFTER status
                """);
        ensureColumn(jdbc, "dispatch_until", """
                ALTER TABLE outboxpro_dead_letter
                ADD COLUMN dispatch_until DATETIME(6) NULL AFTER dispatch_owner
                """);
    }

    /**
     * 在当前数据库缺少指定列时执行 DDL。
     *
     * @param jdbc Spring JDBC 模板
     * @param columnName 待检查的死信表列名
     * @param ddl 创建该列的 DDL
     */
    private void ensureColumn(JdbcTemplate jdbc, String columnName, String ddl) {
        if (columnExists(jdbc, columnName)) {
            return;
        }
        try {
            // DDL 仅在升级旧表时执行，新安装和后续启动都只进行轻量元数据查询。
            jdbc.execute(ddl);
        } catch (DataAccessException error) {
            if (!columnExists(jdbc, columnName)) {
                // 只有列仍不存在时才保留异常；并发实例已经建好列属于成功结果。
                throw error;
            }
        }
    }

    /** 查询当前 schema 的死信表是否存在指定列。 */
    private boolean columnExists(JdbcTemplate jdbc, String columnName) {
        Integer count = jdbc.queryForObject("""
                SELECT COUNT(*)
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'outboxpro_dead_letter'
                  AND COLUMN_NAME = ?
                """, Integer.class, columnName);
        return count != null && count > 0;
    }

    /** 执行一个幂等 SQL 资源。 */
    private void execute(String classpathResource) {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator(
                false, false, "UTF-8", new ClassPathResource(classpathResource));
        populator.execute(dataSource);
    }
}
