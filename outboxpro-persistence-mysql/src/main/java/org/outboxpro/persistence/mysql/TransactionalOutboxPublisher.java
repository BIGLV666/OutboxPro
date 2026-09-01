package org.outboxpro.persistence.mysql;

import org.outboxpro.core.envelope.EventEnvelope;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.event.EventRegistry;
import org.outboxpro.core.OutboxProPublisher;
import org.outboxpro.spi.persistence.OutboxRecord;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.outboxpro.spi.serialization.EventSerializer;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.slf4j.MDC;

/** 默认发布器：在调用方当前事务中只写入 Outbox，不直接访问 RabbitMQ。 */
/**
 * 默认事务发布器，只生成信封并写入当前事务中的 Outbox。
 */
public final class TransactionalOutboxPublisher implements OutboxProPublisher {
    private final EventRegistry registry;
    private final EventSerializer serializer;
    private final OutboxRepository repository;
    private final String producer;

/**
 * 执行该公共 API 定义的操作。
 */
    public TransactionalOutboxPublisher(EventRegistry registry, EventSerializer serializer, OutboxRepository repository, String producer) {
        this.registry = registry; this.serializer = serializer; this.repository = repository; this.producer = producer;
    }
    @Override public <T> EventEnvelope<T> publish(String eventType, T payload) { return publish(eventType, payload, Map.of()); }
    @Override public <T> EventEnvelope<T> publish(String eventType, T payload, Map<String, Object> extensions) {
        EventDefinition<?> raw = registry.require(eventType);
        if (payload != null && !raw.getPayloadType().isInstance(payload)) throw new IllegalArgumentException("Payload type mismatch for " + eventType);
        @SuppressWarnings("unchecked") EventDefinition<T> definition = (EventDefinition<T>) raw;
        EventEnvelope<T> envelope = new EventEnvelope<>(UUID.randomUUID().toString(), eventType, definition.getSchemaVersion(), producer, Instant.now(), MDC.get("traceId"), MDC.get("correlationId"), MDC.get("causationId"), payload, extensions);
        repository.insert(new OutboxRecord(0, envelope.getEventId(), envelope.getEventType(), envelope.getSchemaVersion(), envelope.getProducer(), definition.getRoute().exchange(), definition.getRoute().routingKey(), serializer.serialize(envelope), envelope.getTraceId(), envelope.getCorrelationId(), envelope.getCausationId(), "PENDING", 0, null, null, null));
        return envelope;
    }

    @Override public <T> EventEnvelope<T> publish(Class<T> payloadType, T payload) { return publish(payloadType, payload, Map.of()); }
    @Override public <T> EventEnvelope<T> publish(Class<T> payloadType, T payload, Map<String, Object> extensions) {
        if (payloadType == null) throw new IllegalArgumentException("payloadType must not be null");
        // 类型安全发布：按载荷类型反查唯一事件定义，避免业务代码手写 eventType 字符串。
        EventDefinition<?> definition = registry.requireByPayloadType(payloadType);
        return publish(definition.getEventType(), payload, extensions);
    }
}




