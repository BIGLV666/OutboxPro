package org.outboxpro.core.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标注在事件载荷类上，声明该事件的生产端定义。
 *
 * <p>自动装配会扫描被 {@link org.outboxpro.core.handler.AnnotatedOutboxHandler} 引用的载荷类，
 * 按本注解生成等价于 {@code EventDefinition.builder()...build()} 的事件定义并注册进
 * {@link org.outboxpro.core.event.EventRegistry}，替代手写事件定义 Bean。</p>
 *
 * <p>使用 Builder API 的业务方不需要本注解；两种声明方式可共存，
 * 但同一 eventType 重复注册（或与 Bean 定义冲突）会在启动时快速失败。</p>
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface OutboxEvent {

    /**
     * @return 事件类型；必须与发布、订阅两端使用的事件类型一致
     */
    String eventType();

    /**
     * @return 目标交换机名称
     */
    String exchange();

    /**
     * @return 默认路由键；为空时使用 {@link #eventType()} 作为路由键
     */
    String routingKey() default "";

    /**
     * @return 事件结构版本，写入事件信封的 schemaVersion 字段
     */
    String schemaVersion() default "v1";
}
