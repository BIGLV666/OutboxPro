package org.outboxpro.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.outboxpro.core.envelope.EventEnvelope;
import org.outboxpro.spi.serialization.EventSerializer;

/** 默认 Jackson 序列化器，不启用不安全的任意类反序列化。 */
/**
 * 基于 Jackson 的默认事件序列化器，反序列化使用绑定声明的载荷类型。
 */
public final class JacksonEventSerializer implements EventSerializer {
    private final ObjectMapper objectMapper;
/**
 * 执行该公共 API 定义的操作。
 */
    public JacksonEventSerializer(ObjectMapper objectMapper) { this.objectMapper = objectMapper; }
    @Override public String serialize(EventEnvelope<?> envelope) { try { return objectMapper.writeValueAsString(envelope); } catch (Exception e) { throw new IllegalStateException("Cannot serialize event " + envelope.getEventId(), e); } }
    @Override public <T> EventEnvelope<T> deserialize(String json, Class<T> payloadType) { try { var root = objectMapper.readTree(json); T payload = objectMapper.treeToValue(root.path("payload"), payloadType); return new EventEnvelope<>(root.path("eventId").asText(), root.path("eventType").asText(), root.path("schemaVersion").asText(), root.path("producer").asText(), java.time.Instant.parse(root.path("occurredAt").asText()), root.path("traceId").asText(null), root.path("correlationId").asText(null), root.path("causationId").asText(null), payload, objectMapper.convertValue(root.path("extensions"), java.util.Map.class)); } catch (Exception e) { throw new IllegalStateException("Cannot deserialize event", e); } }
}



