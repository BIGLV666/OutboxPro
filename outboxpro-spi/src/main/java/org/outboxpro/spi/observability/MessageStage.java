package org.outboxpro.spi.observability;

/**
 * 消息生命周期阶段枚举。
 */
public enum MessageStage {
    /** 业务事务内创建并保存 Outbox 记录。 */
    OUTBOX,
    /** Relay 向消息系统发布消息。 */
    PUBLISH,
    /** 消费端从消息系统收到消息。 */
    RECEIVE,
    /** 业务 Handler 执行消息处理逻辑。 */
    HANDLER,
    /** 消息等待或执行下一次重试。 */
    RETRY,
    /** 消息进入死信分类、分派或台账流程。 */
    DEAD_LETTER,
    /** 消费端向消息系统确认处理结果。 */
    ACK
}
