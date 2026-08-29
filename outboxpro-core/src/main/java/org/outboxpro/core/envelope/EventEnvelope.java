package org.outboxpro.core.envelope;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * OutboxPro 在生产端与消费端之间传递的不可变事件信封。
 * 基础元数据由框架维护，业务方只能提供业务载荷和扩展元数据。
 *
 * @param <T> 业务载荷类型
 */
public final class EventEnvelope<T> {
    private final String eventId;
    private final String eventType;
    private final String schemaVersion;
    private final String producer;
    private final Instant occurredAt;
    private final String traceId;
    private final String correlationId;
    private final String causationId;
    private final T payload;
    private final Map<String, Object> extensions;

    /**
     * 创建事件信封。eventId、eventType、schemaVersion 和 producer 不能为空。
     * extensions 会被复制为不可变 Map，防止发布后被调用方修改。
     *
     * @param eventId 事件唯一 ID
     * @param eventType 事件类型
     * @param schemaVersion 事件结构版本
     * @param producer 生产者服务名称
     * @param occurredAt 事件产生时间，为空时使用当前时间
     * @param traceId 链路 Trace ID
     * @param correlationId 业务流程关联 ID
     * @param causationId 上游事件 ID
     * @param payload 业务载荷
     * @param extensions 非敏感扩展元数据
     * @throws IllegalArgumentException 基础字段为空时抛出
     */
    public EventEnvelope(String eventId, String eventType, String schemaVersion, String producer,
                         Instant occurredAt, String traceId, String correlationId, String causationId,
                         T payload, Map<String, Object> extensions) {
        this.eventId = require(eventId, "eventId");
        this.eventType = require(eventType, "eventType");
        this.schemaVersion = require(schemaVersion, "schemaVersion");
        this.producer = require(producer, "producer");
        this.occurredAt = occurredAt == null ? Instant.now() : occurredAt;
        this.traceId = traceId;
        this.correlationId = correlationId;
        this.causationId = causationId;
        this.payload = payload;
        this.extensions = extensions == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(extensions));
    }

    private static String require(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
        return value;
    }

    /** @return 事件唯一 ID。 */
    public String getEventId() { return eventId; }
    /** @return 事件类型。 */
    public String getEventType() { return eventType; }
    /** @return 事件结构版本。 */
    public String getSchemaVersion() { return schemaVersion; }
    /** @return 生产者服务名称。 */
    public String getProducer() { return producer; }
    /** @return 事件产生时间。 */
    public Instant getOccurredAt() { return occurredAt; }
    /** @return Trace ID，可能为空。 */
    public String getTraceId() { return traceId; }
    /** @return 业务流程关联 ID，可能为空。 */
    public String getCorrelationId() { return correlationId; }
    /** @return 上游事件 ID，可能为空。 */
    public String getCausationId() { return causationId; }
    /** @return 类型化业务载荷。 */
    public T getPayload() { return payload; }
    /** @return 不可变扩展元数据快照。 */
    public Map<String, Object> getExtensions() { return extensions; }
}
