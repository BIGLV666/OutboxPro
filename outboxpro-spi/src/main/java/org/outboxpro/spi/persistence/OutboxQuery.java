package org.outboxpro.spi.persistence;

/**
 * Outbox 消息检索条件，供运维查询端点和自定义管理台使用。
 *
 * @param status Outbox 状态（PENDING / PROCESSING / RETRY_WAITING / SENT / DEAD），为空表示不过滤
 * @param eventType 事件类型前缀精确匹配，为空表示不过滤
 * @param offset 分页偏移量，从 0 开始
 * @param limit 单页最大行数；实现应自行设置安全上限
 */
public record OutboxQuery(String status, String eventType, int offset, int limit) {
    /** 允许检索的 Outbox 状态白名单，防止外部输入拼接出意外查询。 */
    public static final java.util.Set<String> ALLOWED_STATUSES =
            java.util.Set.of("PENDING", "PROCESSING", "RETRY_WAITING", "SENT", "DEAD");
}
