package org.outboxpro.spi.deadletter;

/**
 * 框架对消息进入死信流程所作出的稳定分类。
 *
 * <p>枚举值是审计、指标和告警的公共语义契约；扩展策略只能读取，不能重写。</p>
 */
public enum DeadLetterReasonCode {
    /** 业务处理明确声明该消息不可重试。 */
    NON_RETRYABLE_EXCEPTION,
    /** 可重试处理连续失败且已达到订阅配置的最大尝试次数。 */
    RETRY_EXHAUSTED,
    /** 当前订阅未声明消息中的事件类型，继续重试不会成功。 */
    UNKNOWN_EVENT_TYPE,
    /** 消息无法解析为合法的 OutboxPro JSON Envelope。 */
    MALFORMED_MESSAGE,
    /** Handler 或其调用链抛出了未被更细粒度规则覆盖的异常。 */
    HANDLER_FAILURE
}
