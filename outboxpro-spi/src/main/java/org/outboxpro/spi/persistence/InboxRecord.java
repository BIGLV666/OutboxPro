package org.outboxpro.spi.persistence;

import java.time.Instant;

/** Inbox 数据行的最小幂等信息。 */
/**
 * Inbox 数据行模型，保存消费端幂等所需的事件信息。
 */
public record InboxRecord(String eventId, String eventType, String status, int retryCount, Instant receivedTime) { }




