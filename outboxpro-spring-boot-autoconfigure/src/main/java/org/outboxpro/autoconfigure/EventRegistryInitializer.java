package org.outboxpro.autoconfigure;

import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.event.EventRegistry;
import java.util.List;

/** 启动时收集并校验生产端事件定义。 */
/**
 * Spring Bean 事件定义初始化器，启动时收集并注册所有事件定义。
 */
public final class EventRegistryInitializer {
/**
 * 执行该公共 API 定义的操作。
 */
    public EventRegistryInitializer(EventRegistry registry, List<EventDefinition<?>> definitions) { definitions.forEach(registry::register); }
}



