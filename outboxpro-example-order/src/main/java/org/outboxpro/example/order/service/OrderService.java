package org.outboxpro.example.order.service;

import org.outboxpro.core.OutboxProPublisher;
import org.outboxpro.example.order.event.OrderCreated;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

/**
 * 订单服务：业务表写入与事件发布发生在同一个本地事务中。
 *
 * <p>这就是 Outbox 模式的全部——业务代码里没有任何 MQ 连接、确认或补偿逻辑：
 * 事务提交时订单行与 Outbox 记录一起生效，事务回滚时两者一起消失。</p>
 */
@Service
public class OrderService {

    private final JdbcTemplate jdbc;
    private final OutboxProPublisher publisher;

    public OrderService(JdbcTemplate jdbc, OutboxProPublisher publisher) {
        this.jdbc = jdbc;
        this.publisher = publisher;
    }

    /**
     * 创建订单并在同一事务中发布 order.created 事件。
     *
     * @param amount 订单金额；负数订单会触发库存消费失败，用于演示重试与 DLQ
     * @return 新订单标识
     */
    @Transactional
    public long createOrder(BigDecimal amount) {
        jdbc.update("INSERT INTO orders (amount, status) VALUES (?, 'CREATED')", amount);
        Long orderId = jdbc.queryForObject("SELECT LAST_INSERT_ID()", Long.class);
        publisher.publish("order.created", new OrderCreated(orderId, amount));
        return orderId;
    }

    /** @return 订单状态；不存在时返回 null。 */
    public String getStatus(long orderId) {
        return jdbc.query("SELECT status FROM orders WHERE id = ?",
                (rs, rowNum) -> rs.getString(1), orderId)
                .stream().findFirst().orElse(null);
    }
}
