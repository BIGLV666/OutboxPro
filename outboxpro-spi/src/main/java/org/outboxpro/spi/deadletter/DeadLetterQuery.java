package org.outboxpro.spi.deadletter;

/**
 * 死信台账检索条件，供运维查询端点和自定义管理台使用。
 *
 * @param eventType 事件类型精确匹配，为空表示不过滤
 * @param consumerName 消费者名称精确匹配，为空表示不过滤
 * @param status 台账状态过滤，为空表示不过滤
 * @param offset 分页偏移量，从 0 开始
 * @param limit 单页最大行数；实现应自行设置安全上限
 */
public record DeadLetterQuery(String eventType, String consumerName, DeadLetterStatus status, int offset, int limit) {
}
