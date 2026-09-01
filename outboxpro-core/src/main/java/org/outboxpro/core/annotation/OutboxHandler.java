package org.outboxpro.core.annotation;

import org.outboxpro.core.subscription.ConsumeMode;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标注在业务 Handler 上，声明该 Handler 的队列订阅与事件绑定。
 *
 * <p>自动装配会收集所有携带本注解的 {@link org.outboxpro.core.handler.OutboxProHandler} Bean，
 * 按「交换机 + 队列 + 消费者名称」分组生成等价于
 * {@code OutboxProSubscription.builder()...build()} 的订阅，替代手写订阅 Bean。
 * 推荐 Handler 继承 {@link org.outboxpro.core.handler.AnnotatedOutboxHandler}，
 * 从而完全省略 {@code eventType()}/{@code payloadType()}/{@code consumerName()} 样板方法。</p>
 *
 * <p>注解式与 Builder 式订阅可以共存；两者对同一队列声明冲突的绑定时会
 * 在启动拓扑阶段快速失败。</p>
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface OutboxHandler {

    /**
     * @return 该 Handler 消费的事件载荷类型；类上必须标注 {@link OutboxEvent}
     */
    Class<?> event();

    /**
     * @return 业务消费队列名称；一个队列上的多个 Handler 会被合并为同一个订阅
     */
    String queue();

    /**
     * @return 该绑定所在的主交换机；为空时使用事件 {@link OutboxEvent#exchange()}
     */
    String exchange() default "";

    /**
     * @return Inbox 幂等消费者名称；为空时使用框架生成的订阅名称。
     *         同一事件类型被多个消费方（多条订阅）消费时必须显式声明
     */
    String consumerName() default "";

    /**
     * @return 消费模式；Reliable 保证 Inbox 幂等 + 失败重试，Best Effort 失败即忽略
     */
    ConsumeMode mode() default ConsumeMode.RELIABLE;

    /**
     * @return 该事件绑定的重试策略覆盖；未设置任何字段时使用全局 outboxpro.retry 配置
     */
    RetryPolicySpec retry() default @RetryPolicySpec;
}
