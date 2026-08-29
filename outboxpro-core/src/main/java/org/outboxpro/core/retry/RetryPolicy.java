package org.outboxpro.core.retry;

import java.time.Duration;

/**
 * 消息重试策略，统一描述最大次数、初始延迟、倍增系数和最大退避时间。
 * 延迟公式为 initialDelay * multiplier^(attempt - 1)，最终受 maxDelay 限制。
 */
public record RetryPolicy(boolean enabled, int maxAttempts, Duration initialDelay, double multiplier, Duration maxDelay) {
    public RetryPolicy {
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be positive");
        if (initialDelay == null || initialDelay.isNegative()) throw new IllegalArgumentException("initialDelay must be non-negative");
        if (multiplier < 1) throw new IllegalArgumentException("multiplier must be >= 1");
        if (maxDelay == null || maxDelay.isNegative()) throw new IllegalArgumentException("maxDelay must be non-negative");
    }

    /**
     * 按尝试次数计算退避延迟。
     *
     * @param attempt 从 1 开始的尝试次数
     * @return 受 maxDelay 限制的退避延迟
     */
    public Duration delayForAttempt(int attempt) {
        if (attempt <= 1) return initialDelay;
        double millis = initialDelay.toMillis() * Math.pow(multiplier, attempt - 1);
        return Duration.ofMillis(Math.min((long) millis, maxDelay.toMillis()));
    }

    /** @return V1 默认重试策略。 */
    public static RetryPolicy defaults() { return new RetryPolicy(true, 5, Duration.ofSeconds(1), 2, Duration.ofMinutes(5)); }
}
