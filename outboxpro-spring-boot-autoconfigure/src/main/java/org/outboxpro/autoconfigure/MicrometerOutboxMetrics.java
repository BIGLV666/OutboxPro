package org.outboxpro.autoconfigure;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.outboxpro.core.metrics.OutboxMetrics;

/**
 * 基于 Micrometer 的指标上报实现。
 *
 * <p>指标名称遵循路线图约定，标签保持低基数：event_type / producer / consumer / queue，
 * 空值与不可归属的事件统一记为 unknown，绝不写入 eventId、payload 或用户输入。</p>
 *
 * <p>指标清单：</p>
 * <ul>
 *   <li>outboxpro.publish.total / publish.success / publish.failure（标签 event_type、producer）</li>
 *   <li>outboxpro.relay.claimed（无标签）</li>
 *   <li>outboxpro.consume.total / consume.success / consume.failure / consume.retry / consume.dead
 *       （标签 event_type、consumer、queue）</li>
 *   <li>outboxpro.inbox.duplicate（标签 event_type、consumer）</li>
 * </ul>
 */
public final class MicrometerOutboxMetrics implements OutboxMetrics {

    private final MeterRegistry registry;

    /** @param registry Micrometer 注册中心 */
    public MicrometerOutboxMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void publishAttempt(String eventType, String producer) {
        publishCounter("outboxpro.publish.total", eventType, producer).increment();
    }

    @Override
    public void publishSuccess(String eventType, String producer) {
        publishCounter("outboxpro.publish.success", eventType, producer).increment();
    }

    @Override
    public void publishFailure(String eventType, String producer) {
        publishCounter("outboxpro.publish.failure", eventType, producer).increment();
    }

    @Override
    public void relayClaimed(int count) {
        registry.counter("outboxpro.relay.claimed").increment(count);
    }

    @Override
    public void consumeStarted(String eventType, String consumer, String queue) {
        consumeCounter("outboxpro.consume.total", eventType, consumer, queue).increment();
    }

    @Override
    public void consumeSucceeded(String eventType, String consumer, String queue) {
        consumeCounter("outboxpro.consume.success", eventType, consumer, queue).increment();
    }

    @Override
    public void consumeFailed(String eventType, String consumer, String queue) {
        consumeCounter("outboxpro.consume.failure", eventType, consumer, queue).increment();
    }

    @Override
    public void consumeRetried(String eventType, String consumer, String queue) {
        consumeCounter("outboxpro.consume.retry", eventType, consumer, queue).increment();
    }

    @Override
    public void consumeDead(String eventType, String consumer, String queue) {
        consumeCounter("outboxpro.consume.dead", eventType, consumer, queue).increment();
    }

    @Override
    public void inboxDuplicate(String eventType, String consumer) {
        registry.counter("outboxpro.inbox.duplicate",
                Tags.of(Tag.of("event_type", tag(eventType)), Tag.of("consumer", tag(consumer)))).increment();
    }

    /** 生产端指标：标签 event_type + producer。 */
    private io.micrometer.core.instrument.Counter publishCounter(String name, String eventType, String producer) {
        return registry.counter(name,
                Tags.of(Tag.of("event_type", tag(eventType)), Tag.of("producer", tag(producer))));
    }

    /** 消费端指标：标签 event_type + consumer + queue。 */
    private io.micrometer.core.instrument.Counter consumeCounter(
            String name, String eventType, String consumer, String queue) {
        return registry.counter(name,
                Tags.of(Tag.of("event_type", tag(eventType)),
                        Tag.of("consumer", tag(consumer)),
                        Tag.of("queue", tag(queue))));
    }

    /** 标签值规范化：空值记 unknown，保证基数有界。 */
    private static String tag(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }
}
