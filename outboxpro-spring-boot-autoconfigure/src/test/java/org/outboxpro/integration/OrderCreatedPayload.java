package org.outboxpro.integration;

/**
 * 集成测试通用事件载荷。
 *
 * @param orderId 业务订单标识
 */
public record OrderCreatedPayload(long orderId) {
}
