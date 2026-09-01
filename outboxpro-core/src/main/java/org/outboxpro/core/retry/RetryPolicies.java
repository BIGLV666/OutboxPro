package org.outboxpro.core.retry;

import org.outboxpro.core.annotation.RetryPolicySpec;

import java.time.Duration;

/**
 * 注解式重试策略解析工具。
 *
 * <p>Java 注解接口不能声明带参静态方法，因此把 {@link RetryPolicySpec} 注解值
 * 解析为运行时 {@link RetryPolicy} 的逻辑放在本工具类中。</p>
 */
public final class RetryPolicies {

    private RetryPolicies() {
    }

    /**
     * 把注解声明解析为运行时重试策略。
     *
     * @param spec 注解声明
     * @return 任一数值字段被显式设置时返回解析后的策略；全部保持哨兵值时返回 {@code null} 表示不覆盖，
     *         调用方应沿用默认策略
     */
    public static RetryPolicy fromSpec(RetryPolicySpec spec) {
        boolean unset = spec.maxAttempts() == RetryPolicySpec.UNSET
                && spec.initialDelayMillis() == RetryPolicySpec.UNSET
                && spec.multiplier() == RetryPolicySpec.UNSET
                && spec.maxDelayMillis() == RetryPolicySpec.UNSET;
        if (unset) {
            return null;
        }
        RetryPolicy fallback = RetryPolicy.defaults();
        return new RetryPolicy(
                spec.enabled(),
                spec.maxAttempts() == RetryPolicySpec.UNSET ? fallback.maxAttempts() : spec.maxAttempts(),
                spec.initialDelayMillis() == RetryPolicySpec.UNSET
                        ? fallback.initialDelay() : Duration.ofMillis(spec.initialDelayMillis()),
                spec.multiplier() == RetryPolicySpec.UNSET ? fallback.multiplier() : spec.multiplier(),
                spec.maxDelayMillis() == RetryPolicySpec.UNSET
                        ? fallback.maxDelay() : Duration.ofMillis(spec.maxDelayMillis()));
    }
}
