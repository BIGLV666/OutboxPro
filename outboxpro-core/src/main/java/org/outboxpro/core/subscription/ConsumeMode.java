package org.outboxpro.core.subscription;

/**
 * 消费可靠性模式，区分事务可靠消费和失败即确认的尽力消费。
 */
public enum ConsumeMode {
    /** 尽力消费；处理失败后不保留可靠重试状态，适合允许少量丢失的旁路场景。 */
    BEST_EFFORT,
    /** 可靠消费；通过 Inbox、重试和死信流程保证失败消息不会被静默确认。 */
    RELIABLE
}
