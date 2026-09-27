package org.outboxpro.autoconfigure;

import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.outboxpro.spi.transport.TopologyManager;
import java.util.List;

/**
 * Spring Bean 订阅拓扑初始化器，启动时声明所有 RabbitMQ 订阅拓扑。
 *
 * <p>声明前先校验 Retry Queue 总量：每条启用重试的绑定会派生 maxAttempts-1 条队列，
 * 订阅、事件或重试档位过多会让 Broker 队列数量失控（每个队列都是独立的 Erlang 进程
 * 与索引开销）。总量超过 {@code outboxpro.consumer.max-retry-queue-count} 时启动失败，
 * 提示收缩 per-binding 的 maxAttempts 或对该绑定关闭重试，而不是静默压垮 Broker。</p>
 */
public final class SubscriptionTopologyInitializer {
    private final TopologyManager manager;
    private final List<OutboxProSubscription> subscriptions;
    private final int maxRetryQueueCount;

    /**
     * 创建拓扑初始化器。
     *
     * @param manager 拓扑管理器
     * @param subscriptions 全部订阅定义
     * @param maxRetryQueueCount 允许声明的 Retry Queue 总量上限
     */
    public SubscriptionTopologyInitializer(TopologyManager manager, List<OutboxProSubscription> subscriptions,
                                           int maxRetryQueueCount) {
        this.manager = manager;
        this.subscriptions = subscriptions;
        this.maxRetryQueueCount = maxRetryQueueCount;
    }

    /** 执行启动阶段的初始化动作：先校验 Retry Queue 总量，再逐订阅声明拓扑。 */
    public void initialize() {
        int totalRetryQueues = 0;
        for (OutboxProSubscription subscription : subscriptions) {
            for (EventBinding binding : subscription.getBindings()) {
                if (binding.retryPolicy().enabled()) {
                    // 第 maxAttempts 次失败直接进死信，因此每个绑定只派生 maxAttempts-1 条重试队列。
                    totalRetryQueues += Math.max(0, binding.retryPolicy().maxAttempts() - 1);
                }
            }
        }
        if (totalRetryQueues > maxRetryQueueCount) {
            throw new IllegalStateException("OutboxPro would declare " + totalRetryQueues
                    + " retry queues across " + subscriptions.size() + " subscription(s), exceeding "
                    + "outboxpro.consumer.max-retry-queue-count=" + maxRetryQueueCount
                    + ". Lower retry.max-attempts (or per-binding @RetryPolicySpec.maxAttempts), "
                    + "disable retry on low-value bindings, or raise the limit consciously.");
        }
        subscriptions.forEach(manager::declare);
    }
}
