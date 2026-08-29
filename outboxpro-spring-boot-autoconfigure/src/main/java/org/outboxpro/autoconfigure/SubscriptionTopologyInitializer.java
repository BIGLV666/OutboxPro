package org.outboxpro.autoconfigure;

import org.outboxpro.core.subscription.OutboxProSubscription;
import org.outboxpro.spi.transport.TopologyManager;
import java.util.List;

/** 应用启动阶段一次性声明用户订阅拓扑。 */
/**
 * Spring Bean 订阅拓扑初始化器，启动时声明所有 RabbitMQ 订阅拓扑。
 */
public final class SubscriptionTopologyInitializer {
    private final TopologyManager manager; private final List<OutboxProSubscription> subscriptions;
/**
 * 执行该公共 API 定义的操作。
 */
    public SubscriptionTopologyInitializer(TopologyManager manager, List<OutboxProSubscription> subscriptions) { this.manager = manager; this.subscriptions = subscriptions; }
/**
 * 执行启动阶段的初始化动作。
 */
    public void initialize() { subscriptions.forEach(manager::declare); }
}



