package org.outboxpro.spi.observability;

/**
 * 消息生命周期状态枚举。
 */
public enum MessageStatus {
    /** 消息记录或阶段事件已经创建。 */
    CREATED,
    /** 当前阶段正在执行。 */
    PROCESSING,
    /** 当前阶段已经成功完成。 */
    SUCCESS,
    /** 当前阶段执行失败。 */
    FAILED,
    /** 当前消息已进入重试流程。 */
    RETRYING,
    /** 当前消息已进入死信终态或死信处置流程。 */
    DEAD,
    /** 当前消息因幂等或策略判断被忽略。 */
    IGNORED,
    /** 消息系统已收到消费确认。 */
    ACKED
}
