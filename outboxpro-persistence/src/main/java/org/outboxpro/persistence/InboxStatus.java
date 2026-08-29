package org.outboxpro.persistence;

/**
 * Inbox 消费状态枚举，描述幂等记录生命周期。
 */
public enum InboxStatus {
    /** 消息已经进入 Inbox，但业务 Handler 尚未完成。 */
    RECEIVED,
    /** 业务 Handler 已成功完成，后续重复消息应按幂等语义跳过。 */
    SUCCESS,
    /** 本次业务处理失败，是否继续尝试由消费模式和重试策略决定。 */
    FAILED,
    /** 消息因幂等命中或策略判断被忽略，未再次执行业务 Handler。 */
    IGNORED
}
