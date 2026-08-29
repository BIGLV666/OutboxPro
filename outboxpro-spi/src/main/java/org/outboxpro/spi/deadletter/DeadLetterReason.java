package org.outboxpro.spi.deadletter;

/**
 * 框架在死信点确定的不可变失败语义。
 *
 * @param code 稳定的框架原因码
 * @param retryable 框架在死信时是否仍认为该错误可重试
 * @param retryExhausted 是否因重试次数耗尽进入死信
 * @param exceptionType 原始异常的完整类名；无异常时为 {@code null}
 * @param exceptionMessage 已脱敏并截断的异常信息；无异常时为 {@code null}
 */
public record DeadLetterReason(DeadLetterReasonCode code, boolean retryable, boolean retryExhausted,
                               String exceptionType, String exceptionMessage) {
    /**
     * 创建不可变死信原因。
     *
     * @throws IllegalArgumentException 当原因码为空时抛出
     */
    public DeadLetterReason {
        if (code == null) {
            throw new IllegalArgumentException("code must not be null");
        }
    }
}
