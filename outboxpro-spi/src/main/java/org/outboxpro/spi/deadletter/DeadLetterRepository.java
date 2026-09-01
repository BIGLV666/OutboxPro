package org.outboxpro.spi.deadletter;

import java.time.Instant;
import java.util.List;

/**
 * 死信台账持久化扩展点。
 *
 * <p>实现必须以条件更新保证重放认领的并发安全，并在状态迁移时维护待重放计数。</p>
 */
public interface DeadLetterRepository {
    /**
     * 创建死信分派记录，或判断该事件副本是否已经被可靠处置。
     *
     * @param context 不可变死信上下文
     * @return {@code true} 表示调用方必须实际执行死信策略；{@code false} 表示已有可重放副本
     */
    boolean beginDispatch(DeadLetterContext context);

    /**
     * 以租约方式竞争一条死信的分派权。
     *
     * @param context 不可变死信上下文
     * @param owner 当前消费调用的唯一分派持有人
     * @param leaseUntil 分派租约到期时间
     * @return 当前调用是否取得分派权
     */
    default boolean beginDispatch(DeadLetterContext context, String owner, Instant leaseUntil) {
        // 兼容旧仓储实现；支持租约的实现应覆盖此方法。
        return beginDispatch(context);
    }

    /**
     * 释放失败分派留下的租约，使 RabbitMQ 重投递能够再次获得分派权。
     *
     * @param eventId 原始事件 ID
     * @param consumerName 消费者名称
     * @param owner 当前分派持有人
     */
    default void releaseDispatch(String eventId, String consumerName, String owner) {
        // 非数据库仓储可以按自身事务语义覆盖；默认实现保持向后兼容。
    }

    /**
     * 将已可靠交给死信策略的记录转为可人工重放状态。
     *
     * @param eventId 原始事件 ID
     * @param consumerName 消费者名称
     * @param at 成功分派时间
     */
    void markPendingReplay(String eventId, String consumerName, Instant at);

    /**
     * 由当前分派租约持有人将记录转为可人工重放状态。
     *
     * <p>支持分派租约的仓储应覆盖此方法，并通过 {@code owner} 条件更新阻止过期持有人
     * 覆盖新持有人的状态。默认实现用于兼容旧仓储。</p>
     *
     * @param eventId 原始事件 ID
     * @param consumerName 消费者名称
     * @param owner 当前分派租约持有人
     * @param at 成功分派时间
     * @return 是否由当前持有人成功完成状态迁移
     */
    default boolean markPendingReplay(String eventId, String consumerName, String owner, Instant at) {
        // 兼容旧仓储实现；租约感知实现必须使用 owner 做条件更新。
        markPendingReplay(eventId, consumerName, at);
        return true;
    }

    /**
     * 按事件 ID 独占所有仍可重放的死信记录，并消耗一次重放额度。
     *
     * @param eventId 原始事件 ID
     * @param owner 本次重放请求的唯一租约标识
     * @param maxReplayCount 单条记录允许的最大重放次数
     * @param operator 审计操作人
     * @param reason 重放原因
     * @param at 认领时间
     * @return 当前请求成功独占的死信记录；同一事件可能对应多个消费者
     */
    List<DeadLetterRecord> claimReplayByEventId(String eventId, String owner, int maxReplayCount,
                                                String operator, String reason, Instant at);

    /**
     * 标记一条已独占记录的消息重放已被 RabbitMQ 等目标端可靠确认。
     *
     * @param id 死信记录主键
     * @param owner 当前重放租约持有人
     * @param at 确认时间
     */
    void markReplaySucceeded(long id, String owner, Instant at);

    /**
     * 释放重放租约，使投递失败的死信稍后可再次重试。
     *
     * @param id 死信记录主键
     * @param owner 当前重放租约持有人
     * @param error 已脱敏并截断的失败信息
     * @param at 失败时间
     */
    void releaseReplay(long id, String owner, String error, Instant at);

    /**
     * 恢复因重放进程崩溃而超时的 REPLAYING 记录。
     *
     * @param now 当前时间
     * @return 恢复为 PENDING_REPLAY 的记录数量
     */
    default int recoverExpiredReplays(Instant now) {
        // 非数据库仓储可以按自身租约机制覆盖；默认实现保持 SPI 向后兼容。
        return 0;
    }

    /**
     * 返回待人工重放的精确数量；实现应避免扫描不断增长的死信明细表。
     *
     * @return PENDING_REPLAY 状态的精确总数
     */
    long pendingReplayCount();

    /**
     * 按条件分页检索死信台账，供运维查询使用。
     * 返回按 id 倒序排列，条件字段全部精确匹配。
     *
     * @param query 检索条件
     * @return 命中的死信记录（含原始载荷）；不支持检索的实现返回空列表
     */
    default List<DeadLetterRecord> findDeadLetters(DeadLetterQuery query) {
        return List.of();
    }

    /**
     * 统计检索条件命中的死信总数，供运维分页展示。
     *
     * @param query 检索条件
     * @return 命中总数；不支持检索的实现返回 0
     */
    default long countDeadLetters(DeadLetterQuery query) {
        return 0L;
    }
}
