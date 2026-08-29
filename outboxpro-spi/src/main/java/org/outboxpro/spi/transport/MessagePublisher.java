package org.outboxpro.spi.transport;

import org.outboxpro.spi.persistence.OutboxRecord;

/**
 * 消息发布扩展点。
 * 发布失败必须通过 RuntimeException 反馈给 Outbox Relay，避免消息被错误标记为 SENT。
 */
public interface MessagePublisher {
    /** @param record 已认领的 Outbox 记录 @throws RuntimeException 发布或 Confirm 失败时抛出。 */
    void publish(OutboxRecord record);
}
