package org.outboxpro.autoconfigure;

import org.outboxpro.core.annotation.OutboxEvent;
import org.outboxpro.core.annotation.OutboxHandler;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.event.EventRegistry;
import org.outboxpro.core.exception.EventConfigurationException;
import org.outboxpro.core.handler.AnnotatedOutboxHandler;
import org.outboxpro.core.handler.OutboxProHandler;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 注解式装配注册器：启动时把 {@code @OutboxHandler} 注解的 Handler Bean
 * 转换为事件定义与队列订阅，等价于手写 EventDefinition / OutboxProSubscription Bean。
 *
 * <p>处理流程：</p>
 * <ol>
 *   <li>收集容器中所有 {@link OutboxProHandler} Bean，筛选携带 {@link OutboxHandler} 注解的实例；</li>
 *   <li>读取事件载荷类上的 {@link OutboxEvent} 注解，生成事件定义并注册（重复 eventType 快速失败）；</li>
 *   <li>按「交换机 + 队列 + 消费者名称」分组生成 {@link OutboxProSubscription}，
 *       以单例形式注册进容器，供拓扑初始化器与消费管理器发现。</li>
 * </ol>
 *
 * <p>本 Bean 必须先于 SubscriptionTopologyInitializer 与 RabbitConsumerManager 创建，
 * 自动装配通过 {@code @DependsOn} 保证该顺序。</p>
 */
public final class AnnotationDrivenOutboxRegistrar {

    private final EventRegistry registry;
    private final ConfigurableListableBeanFactory beanFactory;

    /**
     * 创建注册器并完成注解式装配。
     *
     * @param registry 事件定义注册表
     * @param handlers 容器中的全部 Handler Bean
     * @param beanFactory 用于注册生成的订阅单例
     */
    public AnnotationDrivenOutboxRegistrar(EventRegistry registry, List<OutboxProHandler<?>> handlers,
                                           ConfigurableListableBeanFactory beanFactory) {
        this.registry = registry;
        this.beanFactory = beanFactory;
        registerAnnotatedDefinitions(handlers);
        for (OutboxProSubscription subscription : buildSubscriptions(handlers).values()) {
            // 注册为普通单例：SubscriptionTopologyInitializer 与 RabbitConsumerManager
            // 通过 ObjectProvider<OutboxProSubscription> 按类型收集，能发现手工注册的单例。
            beanFactory.registerSingleton("outboxpro.subscription." + subscription.getName(), subscription);
        }
    }

    /** 为每个携带注解的 Handler 注册事件定义；已被 Bean 定义注册过时只做一致性校验。 */
    private void registerAnnotatedDefinitions(List<OutboxProHandler<?>> handlers) {
        for (AnnotatedSpec spec : collectSpecs(handlers)) {
            OutboxEvent event = spec.eventAnnotation();
            EventDefinition<?> existing = registry.find(event.eventType());
            if (existing == null) {
                registry.register(new EventDefinition<>(event.eventType(), event.schemaVersion(),
                        spec.payloadType(), new org.outboxpro.core.event.EventRoute(
                        event.exchange(), resolveRoutingKey(event))));
            } else if (!existing.getPayloadType().equals(spec.payloadType())) {
                throw new EventConfigurationException("Event type " + event.eventType()
                        + " is already registered with payload type " + existing.getPayloadType().getName()
                        + ", but handler " + spec.handler().getClass().getName()
                        + " declares " + spec.payloadType().getName());
            }
        }
    }

    /** 按「交换机 + 队列 + 消费者名称」分组构建注解式订阅。 */
    private Map<String, OutboxProSubscription> buildSubscriptions(List<OutboxProHandler<?>> handlers) {
        Map<String, List<AnnotatedSpec>> groups = new LinkedHashMap<>();
        for (AnnotatedSpec spec : collectSpecs(handlers)) {
            OutboxHandler annotation = spec.annotation();
            String exchange = annotation.exchange().isBlank()
                    ? spec.eventAnnotation().exchange() : annotation.exchange();
            String groupKey = exchange + "|" + annotation.queue() + "|" + annotation.consumerName();
            groups.computeIfAbsent(groupKey, ignored -> new ArrayList<>()).add(spec);
        }

        Map<String, OutboxProSubscription> subscriptions = new LinkedHashMap<>();
        for (List<AnnotatedSpec> group : groups.values()) {
            OutboxHandler first = group.get(0).annotation();
            String exchange = first.exchange().isBlank()
                    ? group.get(0).eventAnnotation().exchange() : first.exchange();
            String subscriptionName = "annotated-" + first.queue()
                    + (first.consumerName().isBlank() ? "" : "-" + first.consumerName());
            List<EventBinding> bindings = group.stream().map(spec -> {
                RetryPolicy override = org.outboxpro.core.retry.RetryPolicies.fromSpec(spec.annotation().retry());
                return new EventBinding(spec.eventAnnotation().eventType(), resolveRoutingKey(spec.eventAnnotation()),
                        spec.payloadType(), spec.annotation().mode(), override);
            }).toList();
            OutboxProSubscription.Builder builder = OutboxProSubscription.builder()
                    .name(subscriptionName)
                    .exchange(exchange)
                    .queue(first.queue())
                    .bindings(bindings.toArray(EventBinding[]::new));
            if (!first.consumerName().isBlank()) {
                builder.consumerName(first.consumerName());
            }
            subscriptions.put(subscriptionName, builder.build());
        }
        return subscriptions;
    }

    /** 收集全部注解式 Handler 的解析结果，并校验注解与 Handler 自身契约一致。 */
    private List<AnnotatedSpec> collectSpecs(List<OutboxProHandler<?>> handlers) {
        List<AnnotatedSpec> specs = new ArrayList<>();
        for (OutboxProHandler<?> handler : handlers) {
            Class<?> handlerType = unwrapProxy(handler.getClass());
            OutboxHandler annotation = handlerType.getAnnotation(OutboxHandler.class);
            if (annotation == null) {
                continue;
            }
            OutboxEvent event = annotation.event().getAnnotation(OutboxEvent.class);
            if (event == null) {
                throw new EventConfigurationException("Payload class " + annotation.event().getName()
                        + " must be annotated with @OutboxEvent (handler: " + handlerType.getName() + ")");
            }
            // 直接实现 OutboxProHandler 的注解式 Handler：其 eventType()/payloadType() 必须与注解一致，
            // 否则消费端按注解建绑定、Handler 按方法路由会静默错位。
            if (!(handler instanceof AnnotatedOutboxHandler)) {
                if (!Objects.equals(handler.eventType(), event.eventType())) {
                    throw new EventConfigurationException("Handler " + handlerType.getName()
                            + " returns eventType '" + handler.eventType() + "' but @OutboxHandler declares '"
                            + event.eventType() + "'");
                }
                if (!handler.payloadType().equals(annotation.event())) {
                    throw new EventConfigurationException("Handler " + handlerType.getName()
                            + " returns payloadType '" + handler.payloadType().getName()
                            + "' but @OutboxHandler declares '" + annotation.event().getName() + "'");
                }
            }
            specs.add(new AnnotatedSpec(handler, annotation, event, annotation.event()));
        }
        return specs;
    }

    /** 路由键解析：注解未显式提供时回退为事件类型，与生产端 EventDefinition 保持同构。 */
    private String resolveRoutingKey(OutboxEvent event) {
        return event.routingKey().isBlank() ? event.eventType() : event.routingKey();
    }

    /** 去除 Spring CGLIB 代理外壳，确保能读取到业务类上的注解。 */
    private Class<?> unwrapProxy(Class<?> type) {
        while (type != null && type.getName().contains("$$")) {
            type = type.getSuperclass();
        }
        return type;
    }

    /** 单个注解式 Handler 的解析结果。 */
    private record AnnotatedSpec(OutboxProHandler<?> handler, OutboxHandler annotation,
                                 OutboxEvent eventAnnotation, Class<?> payloadType) { }
}
