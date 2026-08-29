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

    /** @return 当前注册表的不可变快照。 */
    public Map<String, EventDefinition<?>> definitions() { return Map.copyOf(definitions); }
}
