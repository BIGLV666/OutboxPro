package org.outboxpro.transport.rabbit;

import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.outboxpro.spi.transport.TopologyManager;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.core.AmqpAdmin;

/**
 * RabbitMQ 拓扑管理器，自动创建主队列、重试队列和死信队列。
 * Retry Queue 使用 TTL + Dead Letter Exchange 回流到业务 Exchange。
 *
 * <p>Retry Queue 的 TTL 由每个事件绑定自身的 {@link RetryPolicy} 决定：
 * 名为 {@code queue.retry.{eventType}.N} 的队列承载"已失败 N 次、等待第 N+1 次尝试"的消息，
 * TTL 等于 {@code retryPolicy.delayForAttempt(N)}，与生产端 Relay 的退避公式保持一致。</p>
 *
 * <p>依赖 {@link AmqpAdmin} 接口而不是具体的 {@code RabbitAdmin}：
 * 自动装配场景中容器内可能只有 Spring Boot 提供的 {@code amqpAdmin}（声明类型为 AmqpAdmin），
 * 依赖具体类型会在按类型注入时因定义类型不匹配而失败，或与默认 Bean 产生冲突。</p>
 */
public final class RabbitTopologyManager implements TopologyManager {
    private final AmqpAdmin admin;

    /**
     * 创建拓扑管理器。
     *
     * @param admin AMQP 管理客户端
     */
    public RabbitTopologyManager(AmqpAdmin admin) {
        this.admin = admin;
    }

    /**
     * 声明订阅所需的 Exchange、Queue、Binding、Retry Queue、Retry Exchange、DLQ 和 DLX。
     *
     * @param subscription 用户声明的队列订阅
     */
    @Override
    public void declare(OutboxProSubscription subscription) {
        // 1. 声明业务 Exchange、Queue 及 Binding，构成正常消息投递路径
        DirectExchange exchange = new DirectExchange(subscription.getExchange(), true, false);
        admin.declareExchange(exchange);
        Queue queue = QueueBuilder.durable(subscription.getQueue()).build();
        admin.declareQueue(queue);
        for (EventBinding binding : subscription.getBindings()) {
            admin.declareBinding(BindingBuilder.bind(queue).to(exchange).with(binding.routingKey()));
        }

        // 2. 声明 Retry Exchange 及 Retry Queue，利用 TTL + DLX 实现延迟重试。
        //    Retry Queue 按事件绑定分别声明，TTL 来自绑定自身的 RetryPolicy 退避公式；
        //    第 maxAttempts 次失败会直接进入死信流程，因此只需要 1..maxAttempts-1 条重试队列。
        DirectExchange retryExchange = new DirectExchange(retryExchange(subscription), true, false);
        admin.declareExchange(retryExchange);
        for (EventBinding binding : subscription.getBindings()) {
            RetryPolicy retryPolicy = binding.retryPolicy();
            if (!retryPolicy.enabled()) {
                continue;
            }
            for (int attempt = 1; attempt < retryPolicy.maxAttempts(); attempt++) {
                String retryQueueName = retryQueue(subscription, binding, attempt);
                java.util.Map<String, Object> arguments = java.util.Map.of(
                        "x-message-ttl", retryPolicy.delayForAttempt(attempt).toMillis(),
                        "x-dead-letter-exchange", subscription.getExchange(),
                        "x-dead-letter-routing-key", binding.routingKey());
                Queue retryQueue = new Queue(retryQueueName, true, false, false, arguments);
                admin.declareQueue(retryQueue);
                admin.declareBinding(BindingBuilder.bind(retryQueue).to(retryExchange).with(retryQueueName));
            }
        }

        // 3. 声明 Dead Letter Exchange 及 DLQ，所有重试耗尽的消息最终落入此处，便于人工排查。
        //    绑定键必须使用订阅队列名而不是固定字面量：多个订阅共用同一 Exchange 时共享同一个 DLX，
        //    若都用 "dead" 绑定，一条死信会同时进入所有订阅的 DLQ，造成串扰和重复副本。
        TopicExchange deadExchange = new TopicExchange(deadExchange(subscription), true, false);
        admin.declareExchange(deadExchange);
        Queue deadQueue = QueueBuilder.durable(subscription.getQueue() + ".dlq").build();
        admin.declareQueue(deadQueue);
        admin.declareBinding(BindingBuilder.bind(deadQueue).to(deadExchange).with(deadRoutingKey(subscription.getQueue())));
    }

    /**
     * 死信发布使用的路由键：按订阅队列名派生，保证同一 Exchange 下的多条订阅互不串扰。
     *
     * @param queueName 订阅业务队列名
     * @return DLQ 绑定与发布共用的路由键
     */
    public static String deadRoutingKey(String queueName) { return safe(queueName); }

    /** @param subscription 订阅 @return Retry Exchange 名称。 */
    public static String retryExchange(OutboxProSubscription subscription) { return subscription.getExchange() + ".retry"; }
    /** @param subscription 订阅 @return Dead Letter Exchange 名称。 */
    public static String deadExchange(OutboxProSubscription subscription) { return subscription.getExchange() + ".dlx"; }
    /**
     * 生成 Retry Queue 名称。
     * @param subscription 订阅
     * @param binding 事件绑定
     * @param attempt 重试尝试次数
     * @return 稳定且可预测的 Retry Queue 名称
     */
    public static String retryQueue(OutboxProSubscription subscription, EventBinding binding, int attempt) {
        return subscription.getQueue() + ".retry." + safe(binding.eventType()) + "." + attempt;
    }

    private static String safe(String value) { return value.replaceAll("[^A-Za-z0-9_.-]", "_"); }
}

