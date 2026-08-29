package org.outboxpro.persistence;

/**
 * Outbox 消息状态枚举，描述消息投递生命周期。
 */
public enum OutboxStatus {
    /** 消息等待 Relay 认领并发布。 */
    PENDING,
    /** 消息已被某个 Relay 实例独占并正在发布。 */
    PROCESSING,
    /** 上次发布失败，消息等待下一次计划重试。 */
    RETRY_WAITING,
    /** 目标消息系统已经确认接收该消息。 */
    SENT,
    /** 发布重试已耗尽，消息进入终止投递状态。 */
    DEAD
}
