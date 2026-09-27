package org.outboxpro.integration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.outboxpro.autoconfigure.OutboxOpsEndpoint;
import org.outboxpro.autoconfigure.RetentionTask;
import org.outboxpro.spi.deadletter.DlqReplayAuthorizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 保留策略集成测试：RetentionTask 只清理各表的终态行，
 * PENDING / FAILED / PENDING_REPLAY 等活跃状态行不受影响；
 * 手动清理端点与定时调度共享同一套分批删除逻辑。
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, RetentionIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=1h",
                "outboxpro.consumer.enabled=false",
                "outboxpro.ops.enabled=true"
        })
class RetentionIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "retention");
    }

    /** 放行授权器：手动清理端点的 scope 校验在本测试中始终通过。 */
    @Configuration
    static class Config {
        @Bean
        DlqReplayAuthorizer allowAllAuthorizer() {
            return (eventIdOrScope, operator) -> { };
        }
    }

    @Autowired
    RetentionTask retentionTask;

    @Autowired
    OutboxOpsEndpoint opsEndpoint;

    @Autowired
    JdbcTemplate jdbc;

    /** 每个用例先清空相关表：隔离数据库跨运行残留，保证删除计数断言精确。 */
    @BeforeEach
    void cleanTables() {
        jdbc.update("DELETE FROM outboxpro_outbox");
        jdbc.update("DELETE FROM outboxpro_inbox");
        jdbc.update("DELETE FROM outboxpro_message_log");
        jdbc.update("DELETE FROM outboxpro_dead_letter");
        jdbc.update("DELETE FROM outboxpro_dead_letter_counter");
    }

    /** 清理任务必须删除过期终态行，且绝不触碰活跃状态行。 */
    @Test
    void purgeRemovesOnlyTerminalRowsPastRetention() {
        insertOutbox("sent-old", "SENT", "2020-01-01 00:00:00");
        insertOutbox("sent-fresh", "SENT", "2030-01-01 00:00:00");
        insertOutbox("pending-old", "PENDING", null);
        insertInbox("success-old", "SUCCESS", "2020-01-01 00:00:00");
        insertInbox("failed-old", "FAILED", "2020-01-01 00:00:00");
        insertMessageLog("log-old", "2020-01-01 00:00:00");
        insertMessageLog("log-fresh", "2030-01-01 00:00:00");
        insertDeadLetter("dlq-replayed-old", "REPLAYED", "2020-01-01 00:00:00");
        insertDeadLetter("dlq-pending", "PENDING_REPLAY", null);

        retentionTask.purge();

        assertThat(count("outboxpro_outbox", "sent-old")).as("过期 SENT 应被清理").isZero();
        assertThat(count("outboxpro_outbox", "sent-fresh")).as("未到期 SENT 应保留").isOne();
        assertThat(count("outboxpro_outbox", "pending-old")).as("PENDING 不属于清理范围").isOne();
        assertThat(count("outboxpro_inbox", "success-old")).as("过期 SUCCESS 应被清理").isZero();
        assertThat(count("outboxpro_inbox", "failed-old")).as("FAILED 不属于清理范围").isOne();
        assertThat(count("outboxpro_message_log", "log-old")).as("过期日志应被清理").isZero();
        assertThat(count("outboxpro_message_log", "log-fresh")).as("未到期日志应保留").isOne();
        assertThat(count("outboxpro_dead_letter", "dlq-replayed-old")).as("过期 REPLAYED 应被清理").isZero();
        assertThat(count("outboxpro_dead_letter", "dlq-pending")).as("PENDING_REPLAY 不属于清理范围").isOne();
    }

    /** 手动清理端点复用同一套分批删除逻辑，返回各表实际删除行数。 */
    @Test
    void manualPurgeEndpointDeletesTerminalRowsAndReportsCounts() {
        insertOutbox("manual-sent-old", "SENT", "2020-01-01 00:00:00");
        insertOutbox("manual-pending", "PENDING", null);
        insertInbox("manual-success-old", "SUCCESS", "2020-01-01 00:00:00");
        insertMessageLog("manual-log-old", "2020-01-01 00:00:00");
        insertDeadLetter("manual-dlq-replayed", "REPLAYED", "2020-01-01 00:00:00");

        OutboxOpsEndpoint.PurgeOutcome outcome = opsEndpoint.purgeRetention(
                new OutboxOpsEndpoint.OpsRequest("tester", "manual cleanup via ops endpoint"));

        assertThat(outcome.skipped()).as("无并发清理时不应跳过").isFalse();
        assertThat(outcome.outboxSent()).as("Outbox SENT 删除数").isEqualTo(1);
        assertThat(outcome.inboxSuccess()).as("Inbox SUCCESS 删除数").isEqualTo(1);
        assertThat(outcome.messageLog()).as("消息日志删除数").isEqualTo(1);
        assertThat(outcome.deadLetterReplayed()).as("死信 REPLAYED 删除数").isEqualTo(1);

        assertThat(count("outboxpro_outbox", "manual-sent-old")).isZero();
        assertThat(count("outboxpro_outbox", "manual-pending")).as("PENDING 不受手动清理影响").isOne();
        assertThat(count("outboxpro_inbox", "manual-success-old")).isZero();
        assertThat(count("outboxpro_message_log", "manual-log-old")).isZero();
        assertThat(count("outboxpro_dead_letter", "manual-dlq-replayed")).isZero();
    }

    private void insertOutbox(String eventId, String status, String sentTime) {
        jdbc.update("""
                INSERT INTO outboxpro_outbox (event_id, event_type, schema_version, producer,
                    exchange_name, routing_key, payload_json, status, attempt_count, sent_time, version)
                VALUES (?, 'it.retention', 'v1', 'it', 'it.exchange', 'it.retention', '{}', ?, 0, ?, 0)
                """, eventId, status, sentTime);
    }

    private void insertInbox(String eventId, String status, String processedTime) {
        jdbc.update("""
                INSERT INTO outboxpro_inbox (consumer_name, event_id, event_type, status,
                    retry_count, received_time, processed_time, version)
                VALUES ('it-consumer', ?, 'it.retention', ?, 0, NOW(6), ?, 0)
                """, eventId, status, processedTime);
    }

    private void insertMessageLog(String eventId, String occurredTime) {
        jdbc.update("""
                INSERT INTO outboxpro_message_log (event_id, event_type, stage, status, attempt, occurred_time)
                VALUES (?, 'it.retention', 'PUBLISH', 'SUCCESS', 1, ?)
                """, eventId, occurredTime);
    }

    private void insertDeadLetter(String eventId, String status, String replayedTime) {
        jdbc.update("""
                INSERT INTO outboxpro_dead_letter (event_id, event_type, consumer_name, queue_name,
                    original_exchange, original_routing_key, payload_json, attempt_count,
                    reason_code, reason_retryable, reason_retry_exhausted, status,
                    replay_count, replayed_time, version)
                VALUES (?, 'it.retention', 'it-consumer', 'it.queue',
                    'it.exchange', 'it.retention', '{}', 5,
                    'RETRY_EXHAUSTED', 1, 1, ?,
                    1, ?, 0)
                """, eventId, status, replayedTime);
    }

    private long count(String table, String eventId) {
        Long value = jdbc.queryForObject("SELECT COUNT(*) FROM " + table + " WHERE event_id = ?", Long.class, eventId);
        return value == null ? -1 : value;
    }
}
