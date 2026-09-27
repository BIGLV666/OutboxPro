package org.outboxpro.spi.transport;

import org.outboxpro.spi.persistence.OutboxRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * 消息发布扩展点。
 * 发布失败必须通过 RuntimeException 反馈给 Outbox Relay，避免消息被错误标记为 SENT。
 */
public interface MessagePublisher {
    /** @param record 已认领的 Outbox 记录 @throws RuntimeException 发布或 Confirm 失败时抛出。 */
    void publish(OutboxRecord record);

    /**
     * 批量发布一批已认领的 Outbox 记录，返回与输入顺序一一对应的结果。
     *
     * <p>默认实现逐条调用 {@link #publish(OutboxRecord)} 并把 RuntimeException 转换为失败结果，
     * 保证既有自定义实现的二进制兼容。支持管道化的实现（如等待 Publisher Confirm 的
     * RabbitMQ 实现）可覆写本方法：先写出全部消息，再统一等待各条 Confirm，
     * 把 N 次串行往返压缩为一次写出加批量等待。</p>
     *
     * <p>实现必须保证：单条失败不允许影响其他记录的结果判定；
     * 返回列表长度与 {@code records} 一致且顺序对应。</p>
     *
     * @param records 已认领的 Outbox 记录，非空
     * @return 每条记录的发布结果，顺序与输入一致
     */
    default List<PublishOutcome> publishAll(List<OutboxRecord> records) {
        List<PublishOutcome> outcomes = new ArrayList<>(records.size());
        for (OutboxRecord record : records) {
            try {
                publish(record);
                outcomes.add(PublishOutcome.ok());
            } catch (RuntimeException error) {
                outcomes.add(PublishOutcome.failed(error));
            }
        }
        return outcomes;
    }

    /** 单条消息的批量发布结果：成功标记与失败时的异常分类信息。 */
    record PublishOutcome(boolean success, String errorType, String errorMessage) {

        /** @return 成功结果。 */
        public static PublishOutcome ok() {
            return new PublishOutcome(true, null, null);
        }

        /** @param error 发布失败异常 @return 携带异常分类的失败结果。 */
        public static PublishOutcome failed(RuntimeException error) {
            return new PublishOutcome(false, error.getClass().getName(), error.getMessage());
        }
    }
}
