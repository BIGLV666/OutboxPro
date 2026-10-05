package org.outboxpro.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.outboxpro.autoconfigure.OutboxProObservabilitySnapshot;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 积压观测查询压测：默认跳过，使用 {@code -Doutboxpro.performance=true} 开启。
 *
 * <p>构造 5 万条 Outbox 与 5 万条死信台账行，统计共享快照刷新延迟，
 * 并对比待重放原因分布查询“走新复合索引”与“忽略索引全扫”的基线。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@EnabledIfSystemProperty(named = "outboxpro.performance", matches = "true")
@SpringBootTest(
        classes = IntegrationTestApplication.class,
        properties = {
                "outboxpro.producer.relay-enabled=false",
                "outboxpro.consumer.enabled=false",
                "outboxpro.metrics.snapshot-ttl=1ms"
        })
class ObservabilityPerformanceIntegrationTest {

    private static final int HALF = 25_000;

    private static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.4"));

    static {
        MYSQL.start();
    }

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> "4");
    }

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    OutboxProObservabilitySnapshot snapshot;

    @Test
    void snapshotRefreshShouldStayIndexedAndStableUnderBacklog() {
        loadOutboxRows();
        loadDeadLetterRows();

        // 预热 JIT 与 MySQL buffer pool，再统计 30 次快照刷新。
        for (int i = 0; i < 5; i++) {
            snapshot.get();
        }
        double[] latencies = new double[30];
        for (int i = 0; i < latencies.length; i++) {
            sleepMillis(2);
            long started = System.nanoTime();
            var value = snapshot.get();
            latencies[i] = (System.nanoTime() - started) / 1_000_000.0;
            assertThat(value.outboxCount("PENDING")).isEqualTo((long) HALF);
            assertThat(value.pendingReplayCount()).isEqualTo((long) HALF);
        }
        Arrays.sort(latencies);
        double p50 = percentile(latencies, 0.50);
        double p95 = percentile(latencies, 0.95);
        double p99 = percentile(latencies, 0.99);
        System.out.printf("OutboxPro observability snapshot latency ms: p50=%.2f p95=%.2f p99=%.2f max=%.2f%n",
                p50, p95, p99, latencies[latencies.length - 1]);

        // 基线：忽略原因复合索引，强制扫描死信台账，对比索引查询。
        long indexedStart = System.nanoTime();
        jdbc.queryForList("""
                SELECT reason_code, COUNT(*)
                FROM outboxpro_dead_letter
                WHERE status = 'PENDING_REPLAY'
                GROUP BY reason_code
                """);
        double indexedMs = (System.nanoTime() - indexedStart) / 1_000_000.0;

        long baselineStart = System.nanoTime();
        jdbc.queryForList("""
                SELECT reason_code, COUNT(*)
                FROM outboxpro_dead_letter
                IGNORE INDEX (idx_dead_letter_status_reason, idx_dead_letter_replay)
                WHERE status = 'PENDING_REPLAY'
                GROUP BY reason_code
                """);
        double baselineMs = (System.nanoTime() - baselineStart) / 1_000_000.0;
        System.out.printf("Reason-distribution query ms: indexed=%.2f baseline-scan=%.2f%n",
                indexedMs, baselineMs);

        assertThat(p95).as("快照 p95 应保持在低延迟范围").isLessThan(500.0);
        assertThat(indexedMs).as("原因分布查询应使用复合索引且快于全扫基线").isLessThanOrEqualTo(baselineMs);
    }

    private void loadOutboxRows() {
        List<Object[]> pending = new ArrayList<>(HALF);
        List<Object[]> sent = new ArrayList<>(HALF);
        for (int i = 0; i < HALF; i++) {
            pending.add(new Object[]{"perf-pending-" + i, "perf.created"});
            sent.add(new Object[]{"perf-sent-" + i, "perf.created"});
        }
        jdbc.batchUpdate("""
                INSERT INTO outboxpro_outbox (
                    event_id, event_type, schema_version, producer, exchange_name,
                    routing_key, payload_json, status, attempt_count, updated_time, version
                ) VALUES (?, ?, 'v1', 'perf', 'perf.exchange', 'perf.created', '{}', 'PENDING', 0, NOW(), 0)
                """, pending);
        jdbc.batchUpdate("""
                INSERT INTO outboxpro_outbox (
                    event_id, event_type, schema_version, producer, exchange_name,
                    routing_key, payload_json, status, attempt_count, sent_time, updated_time, version
                ) VALUES (?, ?, 'v1', 'perf', 'perf.exchange', 'perf.created', '{}', 'SENT', 1, NOW(), NOW(), 0)
                """, sent);
    }

    private void loadDeadLetterRows() {
        List<Object[]> pending = new ArrayList<>(HALF);
        List<Object[]> replayed = new ArrayList<>(HALF);
        String[] reasons = {"RETRY_EXHAUSTED", "HANDLER_FAILURE", "NON_RETRYABLE_EXCEPTION"};
        for (int i = 0; i < HALF; i++) {
            Object[] row = {"perf-dlq-" + i, "perf.created", "perf-consumer", "perf.queue",
                    "perf.exchange", "perf.created", reasons[i % reasons.length]};
            pending.add(row);
            replayed.add(new Object[]{"perf-replayed-" + i, "perf.created", "perf-consumer", "perf.queue",
                    "perf.exchange", "perf.created", reasons[i % reasons.length]});
        }
        String insert = """
                INSERT INTO outboxpro_dead_letter (
                    event_id, event_type, consumer_name, queue_name,
                    original_exchange, original_routing_key, payload_json,
                    attempt_count, reason_code, reason_retryable, reason_retry_exhausted,
                    status, replay_count, version
                ) VALUES (?, ?, ?, ?, ?, ?, '{}', 1, ?, 1, 0, '%s', 0, 0)
                """;
        jdbc.batchUpdate(insert.formatted("PENDING_REPLAY"), pending);
        jdbc.batchUpdate(insert.formatted("REPLAYED"), replayed);
    }

    private double percentile(double[] sorted, double fraction) {
        int index = (int) Math.ceil(fraction * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
    }

    private void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
