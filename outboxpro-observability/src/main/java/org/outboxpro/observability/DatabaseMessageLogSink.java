package org.outboxpro.observability;

import org.outboxpro.spi.observability.MessageLogSink;
import org.outboxpro.spi.observability.MessageTraceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据库消息日志 Sink：异步队列 + 定时批量写入，日志失败绝不阻断消息主流程。
 *
 * <p>内存保护：内部使用有界队列，队列满时丢弃新日志（有限内存保护），
 * 丢弃数量达到告警间隔时输出一条 WARN。批量写入失败时整批丢弃并限流告警，
 * 保证 outboxpro_message_log 表异常（如未建表）不影响业务。</p>
 *
 * <p>表结构与索引（eventId / traceId / occurredTime）由
 * {@code db/migration/mysql/V1__outboxpro.sql} 定义。</p>
 */
public final class DatabaseMessageLogSink implements MessageLogSink, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DatabaseMessageLogSink.class);

    private static final String INSERT_SQL = """
            INSERT INTO outboxpro_message_log (
                event_id, message_id, event_type, trace_id, correlation_id, causation_id,
                producer, consumer, exchange_name, queue_name, routing_key,
                stage, status, attempt, duration_ms, error_type, error_message, occurred_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbc;
    private final BlockingQueue<MessageTraceRecord> queue;
    private final int batchSize;
    private final ScheduledExecutorService flusher;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong droppedCount = new AtomicLong();
    private final AtomicLong lastDropWarnCount = new AtomicLong();
    private final AtomicBoolean writeFailureLogged = new AtomicBoolean(false);

    /**
     * 创建数据库消息日志 Sink 并启动后台刷新线程。
     *
     * @param jdbc Spring JDBC 模板
     * @param queueCapacity 有界队列容量，超出后丢弃新日志
     * @param batchSize 单次批量写入的最大行数
     * @param flushIntervalMillis 刷新间隔（毫秒）
     */
    public DatabaseMessageLogSink(JdbcTemplate jdbc, int queueCapacity, int batchSize, long flushIntervalMillis) {
        if (queueCapacity <= 0 || batchSize <= 0 || flushIntervalMillis <= 0) {
            throw new IllegalArgumentException("queueCapacity, batchSize and flushIntervalMillis must be positive");
        }
        this.jdbc = jdbc;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.batchSize = batchSize;
        this.flusher = Executors.newScheduledThreadPool(1, runnable -> {
            Thread thread = new Thread(runnable, "outboxpro-message-log-sink");
            thread.setDaemon(true);
            return thread;
        });
        this.flusher.scheduleWithFixedDelay(this::flushBatch, flushIntervalMillis, flushIntervalMillis,
                TimeUnit.MILLISECONDS);
    }

    /** 异步入队；队列满时丢弃并限流告警，绝不阻塞消息主流程。 */
    @Override
    public void append(MessageTraceRecord record) {
        if (record == null || closed.get()) {
            return;
        }
        if (!queue.offer(record)) {
            long dropped = droppedCount.incrementAndGet();
            long lastWarned = lastDropWarnCount.get();
            // 每累计 1000 条输出一次限流告警，避免日志风暴
            if (dropped - lastWarned >= 1000 && lastDropWarnCount.compareAndSet(lastWarned, dropped)) {
                log.warn("Message log sink queue is full; dropped {} records in total", dropped);
            }
        }
    }

    /** 批量落库；失败整批丢弃并限流告警。 */
    private void flushBatch() {
        List<MessageTraceRecord> drained = new ArrayList<>(batchSize);
        queue.drainTo(drained, batchSize);
        if (drained.isEmpty()) {
            return;
        }
        List<Object[]> batch = new ArrayList<>(drained.size());
        for (MessageTraceRecord record : drained) {
            batch.add(toRow(record));
        }
        try {
            jdbc.batchUpdate(INSERT_SQL, batch);
            writeFailureLogged.set(false);
        } catch (RuntimeException writeError) {
            if (writeFailureLogged.compareAndSet(false, true)) {
                log.warn("Message log batch write failed; dropped {} records (subsequent failures suppressed)",
                        batch.size(), writeError);
            }
        }
    }

    /** 停止后台线程并把队列中剩余记录尽量刷入数据库（宕机时未刷入的记录按设计丢弃）。 */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        flusher.shutdown();
        try {
            flusher.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // 最终兜底：同步刷一次剩余记录，尽量减少进程正常退出时的日志丢失
        flushBatch();
        long remaining = droppedCount.get();
        if (remaining > 0) {
            log.info("Message log sink closed; {} records were dropped in total due to backpressure", remaining);
        }
    }

    /** 构造单行插入参数，超长字段截断以匹配表结构定义。 */
    Object[] toRow(MessageTraceRecord r) {
        return new Object[]{
                truncate(r.eventId(), 100), truncate(r.messageId(), 100), truncate(r.eventType(), 200),
                truncate(r.traceId(), 128), truncate(r.correlationId(), 128), truncate(r.causationId(), 128),
                truncate(r.producer(), 200), truncate(r.consumer(), 200), truncate(r.exchange(), 255),
                truncate(r.queue(), 255), truncate(r.routingKey(), 255),
                r.stage() == null ? null : r.stage().name(),
                r.status() == null ? null : r.status().name(),
                r.attempt(), r.durationMs(),
                truncate(r.errorType(), 300), truncate(r.errorMessage(), 2000),
                r.occurredAt() == null ? null : Timestamp.from(r.occurredAt())
        };
    }

    /** 截断超长文本，匹配 DDL 列宽并防止单行日志撑大数据库。 */
    private static String truncate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }
}
