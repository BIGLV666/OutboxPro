package org.outboxpro.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.outboxpro.autoconfigure.OutboxRelayScheduler;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.persistence.OutboxRelay;
import org.outboxpro.spi.persistence.OutboxRecord;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.outboxpro.spi.transport.MessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Relay 吞吐基线（默认不执行）。
 *
 * <p>运行方式：{@code mvn -pl outboxpro-spring-boot-autoconfigure -am test
 * -Dtest=OutboxRelayThroughputTest -Doutboxpro.performance=true}。
 * 使用真实 MySQL 容器与零延迟 Publisher，只衡量“认领 + 状态回写 + 批量发布调度”的
 * 单实例上限，不含 RabbitMQ 网络往返。结果仅用于同一机器上的相对比较。</p>
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "outboxpro.performance", matches = "true")
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = IntegrationTestApplication.class,
        properties = {
                "outboxpro.producer.relay-enabled=false",
                "outboxpro.consumer.enabled=false",
                "outboxpro.retention.enabled=false"
        })
class OutboxRelayThroughputTest extends AbstractOutboxProIntegrationTest {

    static {
        // 4 个 worker 需要并行持有连接；默认测试连接池 3 会人为压低多 worker 结果。
        System.setProperty("outboxpro.test.pool-size", "8");
    }

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "throughput");
    }

    private static final int RECORD_COUNT = 2_000;
    private static final long DRAIN_TIMEOUT_SECONDS = 60;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    OutboxRepository outboxRepository;

    @Test
    void relayThroughputBaseline() {
        for (int batchSize : new int[]{100, 500}) {
            measured("workers=1", batchSize, 1, batchSize);
        }
        for (int workers : new int[]{1, 2, 4}) {
            measured("workers=" + workers, 500, workers, 500);
        }
    }

    /** 清空并预置数据，用实际发布的调度器（单 Relay + N worker）排空并打印吞吐。 */
    private void measured(String label, int batchSize, int workers, int measuredBatchSize) {
        jdbc.update("DELETE FROM outboxpro_outbox");
        preload(RECORD_COUNT);
        long elapsedMillis = drainWithScheduler(workers, measuredBatchSize);
        assertThat(countSent()).as("%s, batchSize=%d 时应全部投递", label, batchSize).isEqualTo(RECORD_COUNT);
        double recordsPerSecond = RECORD_COUNT * 1000.0 / Math.max(1, elapsedMillis);
        System.out.printf("OutboxRelay baseline: %s, batchSize=%d, records=%d, elapsed=%dms, throughput=%.0f records/s%n",
                label, batchSize, RECORD_COUNT, elapsedMillis, recordsPerSecond);
    }

    /** 使用生产同款 OutboxRelayScheduler（共享 Relay、N worker、空闲退避）排空全部记录。 */
    private long drainWithScheduler(int workerCount, int batchSize) {
        OutboxRelay relay = new OutboxRelay(outboxRepository, new NoopPublisher(),
                RetryPolicy.defaults(), batchSize, Duration.ofSeconds(30));
        OutboxRelayScheduler scheduler = new OutboxRelayScheduler(
                relay, workerCount, Duration.ofMillis(1), Duration.ofMillis(5));
        long startedNanos = System.nanoTime();
        try {
            scheduler.start();
            long deadline = startedNanos + TimeUnit.SECONDS.toNanos(DRAIN_TIMEOUT_SECONDS);
            while (countSent() < RECORD_COUNT) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("relay drain timed out after "
                            + DRAIN_TIMEOUT_SECONDS + "s: sent=" + countSent());
                }
                Thread.sleep(20);
            }
            return (System.nanoTime() - startedNanos) / 1_000_000;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while measuring relay throughput", interrupted);
        } finally {
            scheduler.stop();
        }
    }

    private int countSent() {
        Integer sent = jdbc.queryForObject(
                "SELECT COUNT(*) FROM outboxpro_outbox WHERE status = 'SENT'", Integer.class);
        return sent == null ? 0 : sent;
    }

    private void preload(int count) {
        List<Object[]> rows = java.util.stream.IntStream.range(0, count)
                .mapToObj(index -> new Object[]{
                        "perf-" + UUID.randomUUID(),
                        "perf.event",
                        "v1",
                        "perf-producer",
                        "perf.exchange",
                        "perf.event",
                        "{\"value\":" + index + "}"})
                .toList();
        jdbc.batchUpdate("""
                INSERT INTO outboxpro_outbox (
                    event_id, event_type, schema_version, producer,
                    exchange_name, routing_key, payload_json,
                    status, attempt_count, updated_time, version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, NOW(), 0)
                """, rows);
    }

    /** 零延迟发布器：只模拟 Confirm 成功，隔离掉 Broker 网络耗时。 */
    private static final class NoopPublisher implements MessagePublisher {
        @Override public void publish(OutboxRecord record) { }
        @Override public List<PublishOutcome> publishAll(List<OutboxRecord> records) {
            return records.stream().map(ignored -> PublishOutcome.ok()).toList();
        }
    }
}
