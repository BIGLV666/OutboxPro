package org.outboxpro.spi.deadletter;

/**
 * 死信记录的生命周期状态。
 *
 * <p>状态只描述框架对死信副本和人工重放的管理，不表示原始业务 Handler 已处理成功。</p>
 */
public enum DeadLetterStatus {
    /** 已建立死信记录，正在将副本交给框架或用户定义的死信处理策略。 */
    DISPATCHING,
    /** 死信副本已被可靠接收，可由人工重放。 */
    PENDING_REPLAY,
    /** 某个重放请求已经独占该记录并正在重新发布。 */
    REPLAYING,
    /** 重放副本已收到目标发布端的确认；业务消费结果仍需通过 Inbox 和消息日志判断。 */
    REPLAYED
}
