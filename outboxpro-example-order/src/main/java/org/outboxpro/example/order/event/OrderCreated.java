package org.outboxpro.example.order.event;

import java.math.BigDecimal;

/**
 * 订单创建事件载荷。
 *
 * @param orderId 订单标识
 * @param amount 订单金额，负数会触发库存消费失败（用于演示重试与 DLQ）
 */
public record OrderCreated(long orderId, BigDecimal amount) {
}
