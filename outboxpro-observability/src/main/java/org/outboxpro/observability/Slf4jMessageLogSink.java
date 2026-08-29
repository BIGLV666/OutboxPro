package org.outboxpro.observability;

import org.outboxpro.spi.observability.MessageLogSink;
import org.outboxpro.spi.observability.MessageTraceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 使用应用现有 Logback 配置输出结构化消息生命周期日志。 */
/**
 * 默认 SLF4J 消息日志 Sink，日志故障会被隔离。
 */
public final class Slf4jMessageLogSink implements MessageLogSink {
    private static final Logger LOG = LoggerFactory.getLogger("OUTBOX_PRO_MESSAGE");
    @Override public void append(MessageTraceRecord record) {
        try {
            LOG.info("eventId={} eventType={} producer={} consumer={} exchange={} queue={} stage={} status={} attempt={} durationMs={} traceId={} errorType={} errorMessage={}", record.eventId(), record.eventType(), record.producer(), record.consumer(), record.exchange(), record.queue(), record.stage(), record.status(), record.attempt(), record.durationMs(), record.traceId(), record.errorType(), record.errorMessage());
        } catch (RuntimeException ignored) {
            // 观测链路故障不能影响业务消息处理。
        }
    }
}




