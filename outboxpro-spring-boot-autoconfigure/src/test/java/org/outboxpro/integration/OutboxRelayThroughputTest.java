package org.outboxpro.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "throughput");
    }

    private static final int RECORD_COUNT = 2_000;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    OutboxRepository outboxRepository;

    @Test
    void relayThroughputBaseline() {
        for (int batchSize : new int[]{100, 500}) {
            jdbc.update("DELETE FROM outboxpro_outbox");
            preload(RECORD_COUNT);

            OutboxRelay relay = new OutboxRelay(outboxRepository, new NoopPublisher(),
                    RetryPolicy.defaults(), batchSize, Duration.ofSeconds(30));

            long started = System.nanoTime();
            int cycles = 0;
            while (cycles++ < 10_000) {
                if (!relay.relayOnceAndReportActivity()) {
                    break;
                }
            }
            long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

            Integer sent = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM outboxpro_outbox WHERE status = 'SENT'", Integer.class);
            assertThat(sent).as("batchSize=%d 时应全部投递", batchSize).isEqualTo(RECORD_COUNT);

            double recordsPerSecond = RECORD_COUNT * 1000.0 / Math.max(1, elapsedMillis);
            System.out.printf("OutboxRelay baseline: workers=1, batchSize=%d, records=%d, elapsed=%dms, throughput=%.0f records/s%n",
                    batchSize, RECORD_COUNT, elapsedMillis, recordsPerSecond);
        }

        for (int workers : new int[]{1, 2, 4}) {
            jdbc.update("DELETE FROM outboxpro_outbox");
            preload(RECORD_COUNT);
            long elapsedMillis = drainWithWorkers(workers, 500);
            double recordsPerSecond = RECORD_COUNT * 1000.0 / Math.max(1, elapsedMillis);
            System.out.printf("OutboxRelay baseline: workers=%d, batchSize=500, records=%d, elapsed=%dms, throughput=%.0f records/s%n",
                    workers, RECORD_COUNT, elapsedMillis, recordsPerSecond);
        }
    }

    /** 启动 N 个独立 Relay 并发认领，直到全部记录 SENT。 */
    private long drainWithWorkers(int workerCount, int batchSize) {
        List<OutboxRelay> relays = IntStream.range(0, workerCount)
                .mapToObj(ignored -> new OutboxRelay(outboxRepository, new NoopPublisher(),
                        RetryPolicy.defaults(), batchSize, Duration.ofSeconds(30)))
                .toList();
        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        AtomicBoolean done = new AtomicBoolean();
        AtomicReference<Throwable> workerError = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(workerCount);
        long startedNanos = System.nanoTime();
        try {
            for (OutboxRelay relay : relays) {
                pool.submit(() -> {
                    started.countDown();
                    try {
                        while (!done.get()) {
                            if (!relay.relayOnceAndReportActivity()) {
                                try {
                                    Thread.sleep(5);
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            }
                        }
                    } catch (Throwable error) {
                        // 静默死亡会让基准结果严重失真，必须把异常带回测试线程。
                        workerError.compareAndSet(null, error);
                        done.set(true);
                    }
                });
            }
            await(started);
            while (countSent() < RECORD_COUNT) {
                if (workerError.get() != null) {
                    throw new IllegalStateException("relay worker failed", workerError.get());
                }
                Thread.sleep(20);
            }
            assertThat(countSent()).as("workers=%d 时应全部投递", workerCount).isEqualTo(RECORD_COUNT);
            return (System.nanoTime() - startedNanos) / 1_000_000;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while measuring relay throughput", interrupted);
        } finally {
            done.set(true);
            pool.shutdown();
            try {
                if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                pool.shutdownNow();
            }
        }
    }

    private void await(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("relay workers did not start in time");
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
