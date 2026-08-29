package org.outboxpro.example.order.handler;

import org.outboxpro.core.context.EventContext;
import org.outboxpro.core.handler.OutboxProHandler;
import org.outboxpro.example.order.event.OrderCreated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 通知 Handler（Best Effort 模式）。
 *
 * <p>Best Effort 消费失败会被框架记为 IGNORED 并直接 ACK，不重试、不阻塞队列——
 * 适合通知、缓存刷新等允许丢失的场景。这里只打印日志模拟发送通知。</p>
 */
@Component
public class NotificationHandler implements OutboxProHandler<OrderCreated> {

    private static final Logger log = LoggerFactory.getLogger(NotificationHandler.class);

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
        return "notification-service";
    }

    @Override
    public void handle(EventContext<OrderCreated> context) {
        OrderCreated event = context.getPayload();
        log.info("[notification] order {} created, notifying user (eventId={})", event.orderId(), context.getEventId());
    }
}
