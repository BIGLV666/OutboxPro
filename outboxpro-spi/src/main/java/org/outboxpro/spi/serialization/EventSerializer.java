package org.outboxpro.spi.serialization;

import org.outboxpro.core.envelope.EventEnvelope;

/** 事件序列化扩展点，负责事件信封与 JSON 之间的转换。 */
public interface EventSerializer {
    /** @param envelope 待序列化信封 @return JSON 字符串 @throws RuntimeException 序列化失败时抛出。 */
    String serialize(EventEnvelope<?> envelope);
    /** @param json JSON 字符串 @param payloadType 目标载荷类型 @param <T> 载荷类型 @return 类型化事件信封 @throws RuntimeException 解析失败时抛出。 */
    <T> EventEnvelope<T> deserialize(String json, Class<T> payloadType);
}
