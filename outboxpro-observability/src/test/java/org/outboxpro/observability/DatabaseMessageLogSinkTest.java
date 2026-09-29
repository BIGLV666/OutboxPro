package org.outboxpro.observability;

import org.junit.jupiter.api.Test;
import org.outboxpro.core.metrics.OutboxMetrics;
import org.outboxpro.spi.observability.MessageStage;
import org.outboxpro.spi.observability.MessageStatus;
import org.outboxpro.spi.observability.MessageTraceRecord;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link DatabaseMessageLogSink} 降级观测单元测试：
 * 队列满丢弃与批量写入失败必须同时被限流日志和指标门面感知。
 */
class DatabaseMessageLogSinkTest {

    /** 记录降级指标上报次数的门面桩。 */
    private static final class RecordingMetrics implements OutboxMetrics {
        final AtomicLong dropped = new AtomicLong();
        final AtomicLong flushFailed = new AtomicLong();

        @Override public void logSinkDropped(int count) { dropped.addAndGet(count); }
        @Override public void logSinkFlushFailed(int count) { flushFailed.addAndGet(count); }
    }

    private static MessageTraceRecord record(String eventId) {
        return new MessageTraceRecord(eventId, null, "it.event", null, null, null,
                "it-producer", "it-consumer", "it.exchange", "it.queue", null,
                MessageStage.HANDLER, MessageStatus.SUCCESS, 1, 1L, null, null, Instant.now());
    }

    private static boolean awaitTrue(java.util.function.BooleanSupplier condition, long timeoutSeconds) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(20);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return condition.getAsBoolean();
    }

    /** 批量写入失败（无 DataSource 的 JdbcTemplate 必然失败）时上报 flushFailed 指标。 */
    @Test
    void flushFailureIsReportedThroughMetrics() {
        RecordingMetrics metrics = new RecordingMetrics();
        DatabaseMessageLogSink sink = new DatabaseMessageLogSink(new JdbcTemplate(), 100, 10, 30, metrics);
        try {
            sink.append(record("flush-fail-1"));
            assertThat(awaitTrue(() -> metrics.flushFailed.get() >= 1, 5))
                    .as("批量写入失败必须上报 logSinkFlushFailed 指标").isTrue();
        } finally {
            sink.close();
        }
    }

    /** 容量为 1 的队列被快速灌满时，被丢弃的记录必须上报 dropped 指标。 */
    @Test
    void queueOverflowIsReportedThroughMetrics() {
        RecordingMetrics metrics = new RecordingMetrics();
        // 容量 1 + 较慢刷新：快速追加必然触发丢弃路径。
        DatabaseMessageLogSink sink = new DatabaseMessageLogSink(new JdbcTemplate(), 1, 1, 50, metrics);
        try {
            for (int i = 0; i < 200; i++) {
                sink.append(record("overflow-" + i));
            }
            assertThat(awaitTrue(() -> metrics.dropped.get() > 0, 5))
                    .as("队列满丢弃必须上报 logSinkDropped 指标").isTrue();
        } finally {
            sink.close();
        }
    }
}
