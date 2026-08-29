package org.outboxpro.integration;

import org.junit.jupiter.api.Test;
import org.outboxpro.core.OutboxProPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 清单 T38：outboxpro.schema-initialize=false → 框架不执行 DDL，表由 Flyway/Liquibase/DBA 管理。
 *
 * <p>按设计，关闭自动建表后上下文仍正常启动（Bean 装配不依赖表存在），
 * 但框架表不会被创建——由运维流程保证。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class},
        properties = {
                "outboxpro.schema-initialize=false",
                "outboxpro.producer.relay-enabled=false",
                "outboxpro.consumer.enabled=false",
                "outboxpro.dlq.ledger.enabled=false"
        })
class SchemaInitializeDisabledIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "schemaoff");
    }

    @Autowired
    OutboxProPublisher publisher;

    @Autowired
    JdbcTemplate jdbc;

    /** T38：关闭自动建表后，上下文正常启动且不创建任何框架表。 */
    @Test
    void schemaInitializeOffCreatesNoTables() {
        // Bean 装配不受影响
        assertThat(publisher).isNotNull();

        Integer tableCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                        + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE 'outboxpro_%'",
                Integer.class);
        assertThat(tableCount).as("框架不应创建任何 outboxpro 表").isZero();
    }
}
