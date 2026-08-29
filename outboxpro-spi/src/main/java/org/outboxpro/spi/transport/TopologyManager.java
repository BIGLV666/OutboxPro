package org.outboxpro.spi.transport;

import org.outboxpro.core.subscription.OutboxProSubscription;

/** 消息拓扑声明扩展点。 */
public interface TopologyManager {
    /** @param subscription 用户声明的订阅。 */
    void declare(OutboxProSubscription subscription);
}
