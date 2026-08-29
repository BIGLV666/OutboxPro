package org.outboxpro.example.order.handler;

import org.outboxpro.core.context.EventContext;
import org.outboxpro.core.handler.OutboxProHandler;
import org.outboxpro.example.order.event.OrderCreated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 库存 Handler（Reliable 模式）。
 *
 * <p>业务更新与 Inbox 状态在框架管理的同一个本地事务中提交，提交成功后才 ACK。
 * 负金额订单会持续失败：先经 Retry Queue 退避重试，耗尽后进入 DLQ 等待人工重放。</p>
 */
@Component
public class InventoryHandler implements OutboxProHandler<OrderCreated> {

    private static final Logger log = LoggerFactory.getLogger(InventoryHandler.class);

    private final JdbcTemplate jdbc;

    public InventoryHandler(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public String eventType() {
        return "order.created";
    }

    @Override
    public Class<OrderCreated> payloadType() {
        return OrderCreated.class;
    }

    /** 同一事件类型被多个订阅消费时，用 consumerName 把 Handler 绑定到所属订阅。 */
    @Override
    public String consumerName() {
        return "inventory-service";
    }

    @Override
    public void handle(EventContext<OrderCreated> context) {
        OrderCreated event = context.getPayload();
        if (event.amount().signum() < 0) {
            // 演示用：负金额视为库存不足，抛出异常触发框架重试
            throw new IllegalStateException("insufficient inventory for order " + event.orderId());
        }
        jdbc.update("INSERT INTO inventory_reservation (order_id) VALUES (?)", event.orderId());
        log.info("[inventory] reserved for order {} (eventId={})", event.orderId(), context.getEventId());
    }
}
