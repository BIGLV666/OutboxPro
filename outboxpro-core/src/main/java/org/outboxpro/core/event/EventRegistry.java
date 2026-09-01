package org.outboxpro.core.event;

import org.outboxpro.core.exception.EventConfigurationException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 事件定义注册表，负责启动校验和发布时的事件查找。
 * 注册表拒绝重复 eventType，避免不同 Bean 对同一事件产生不确定路由。
 */
public final class EventRegistry {
    private final Map<String, EventDefinition<?>> definitions = new LinkedHashMap<>();

    /**
     * 注册事件定义并拒绝重复 eventType。
     *
     * @param definition 待注册定义
     * @throws EventConfigurationException eventType 已经注册时抛出
     */
    public synchronized void register(EventDefinition<?> definition) {
        EventDefinition<?> previous = definitions.putIfAbsent(definition.getEventType(), definition);
        if (previous != null) throw new EventConfigurationException("Duplicate event definition: " + definition.getEventType());
    }

    /**
     * 获取已注册事件定义，未找到时抛出配置异常。
     *
     * @param eventType 事件类型
     * @return 已注册定义
     * @throws EventConfigurationException 事件未注册时抛出
     */
    public EventDefinition<?> require(String eventType) {
        EventDefinition<?> definition = definitions.get(eventType);
        if (definition == null) throw new EventConfigurationException("Event is not registered: " + eventType);
        return definition;
    }

    /**
     * 按事件类型查找定义，不存在时返回 {@code null}。
     * 供注解式装配判断「已由 Bean 定义注册」的场景，避免重复注册误报。
     *
     * @param eventType 事件类型
     * @return 已注册定义；未注册时为 {@code null}
     */
    public EventDefinition<?> find(String eventType) {
        return definitions.get(eventType);
    }

    /**
     * 按载荷类型反查事件定义，用于类型安全发布 {@code publish(Class, payload)}。
     *
     * @param payloadType 载荷 Java 类型
     * @return 唯一匹配的事件定义
     * @throws EventConfigurationException 没有定义或多个定义使用同一载荷类型时抛出；
     *         多个定义时业务方必须改用 eventType 字符串重载消除歧义
     */
    public EventDefinition<?> requireByPayloadType(Class<?> payloadType) {
        EventDefinition<?> matched = null;
        for (EventDefinition<?> definition : definitions.values()) {
            if (definition.getPayloadType().equals(payloadType)) {
                if (matched != null) {
                    throw new EventConfigurationException(
                            "Payload type " + payloadType.getName() + " is registered for multiple events: "
                                    + matched.getEventType() + ", " + definition.getEventType()
                                    + "; use publish(eventType, payload) instead");
                }
                matched = definition;
            }
        }
        if (matched == null) {
            throw new EventConfigurationException("No event registered for payload type " + payloadType.getName());
        }
        return matched;
    }

    /** @return 当前注册表的不可变快照。 */
    public Map<String, EventDefinition<?>> definitions() { return Map.copyOf(definitions); }
}
