package org.outboxpro.core.handler;

import org.outboxpro.core.annotation.OutboxEvent;
import org.outboxpro.core.annotation.OutboxHandler;

/**
 * 注解式 Handler 基类：从自身 {@link OutboxHandler} 注解推导
 * {@link #eventType()}、{@link #payloadType()} 和 {@link #consumerName()}，
 * 业务子类只需要实现 {@link #handle(EventContext)}。
 *
 * <p>子类必须在类上标注 {@code @OutboxHandler(event = XxxPayload.class, queue = "...")}，
 * 且载荷类必须标注 {@link OutboxEvent}；构造阶段完成这些校验，
 * 配置缺失会在应用启动时快速失败。</p>
 *
 * @param <T> 事件载荷类型
 */
public abstract class AnnotatedOutboxHandler<T> implements OutboxProHandler<T> {

    private final String eventType;
    private final Class<T> payloadType;
    private final String consumerName;

    /** 从子类注解解析事件契约；配置不完整时抛出异常阻止应用启动。 */
    protected AnnotatedOutboxHandler() {
        OutboxHandler spec = findOwnAnnotation();
        if (spec == null) {
            throw new IllegalStateException("Handler " + getClass().getName()
                    + " must be annotated with @OutboxHandler");
        }
        OutboxEvent event = spec.event().getAnnotation(OutboxEvent.class);
        if (event == null) {
            throw new IllegalStateException("Payload class " + spec.event().getName()
                    + " must be annotated with @OutboxEvent");
        }
        this.eventType = event.eventType();
        @SuppressWarnings("unchecked")
        Class<T> typedPayload = (Class<T>) spec.event();
        this.payloadType = typedPayload;
        this.consumerName = spec.consumerName().isBlank() ? null : spec.consumerName();
    }

    @Override
    public final String eventType() {
        return eventType;
    }

    @Override
    public final Class<T> payloadType() {
        return payloadType;
    }

    @Override
    public String consumerName() {
        return consumerName;
    }

    /** 读取子类类上的 {@code @OutboxHandler} 注解；代理类会导致注解不可见，这里按原始类查找。 */
    private OutboxHandler findOwnAnnotation() {
        Class<?> type = getClass();
        // Spring CGLIB 子类代理不会复制类型注解，沿父类向上找到第一个声明即可命中原始类。
        while (type != null && type != Object.class) {
            OutboxHandler annotation = type.getAnnotation(OutboxHandler.class);
            if (annotation != null) {
                return annotation;
            }
            type = type.getSuperclass();
        }
        return null;
    }
}
