package org.outboxpro.spi.observability;

import java.time.Instant;

/** 默认不携带完整 Payload 的消息生命周期记录。 */
/**
 * 不包含完整 Payload 的消息生命周期记录。
 */
public record MessageTraceRecord(String eventId, String messageId, String eventType, String traceId, String correlationId,
                                 String causationId, String producer, String consumer, String exchange, String queue,
                                 String routingKey, MessageStage stage, MessageStatus status, int attempt, Long durationMs,
                                 String errorType, String errorMessage, Instant occurredAt) { }




