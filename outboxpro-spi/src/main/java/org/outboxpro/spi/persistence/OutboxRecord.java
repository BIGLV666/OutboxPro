package org.outboxpro.spi.persistence;

import java.time.Instant;

/** Outbox 数据行；payloadJson 已序列化，便于数据库事务与 MQ Relay 解耦。 */
/**
 * Outbox 数据行模型，保存已经序列化的事件和投递状态。
 */
public record OutboxRecord(long id, String eventId, String eventType, String schemaVersion, String producer,
                           String exchangeName, String routingKey, String payloadJson, String traceId,
                           String correlationId, String causationId, String status, int attemptCount,
                           Instant nextRetryTime, String claimOwner, Instant claimedTime) { }




