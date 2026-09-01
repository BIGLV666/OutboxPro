package org.outboxpro.core.annotation;

import org.outboxpro.core.retry.RetryPolicy;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.time.Duration;

/**
 * 注解式重试策略声明。
 *
 * <p>Java 注解属性只能使用编译期常量，无法直接表达 {@link Duration}，
 * 因此时间字段统一使用毫秒。所有数值字段默认为 {@code -1}（哨兵值）：
 * 全部保持默认时表示「不覆盖」，绑定沿用全局 {@code outboxpro.retry} 对应的默认策略；
 * 任一字段被显式设置时，整个策略按注解值构建，未设置的回退到框架默认值。</p>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface RetryPolicySpec {

    /** 哨兵值：表示该字段未在注解中显式设置。 */
    long UNSET = -1L;

    /**
     * @return 是否启用重试；关闭后失败消息直接进入死信流程
     */
    boolean enabled() default true;

    /**
     * @return 最大消费尝试次数；{@code -1} 表示未设置
     */
    int maxAttempts() default -1;

    /**
     * @return 初始重试延迟毫秒数；{@code -1} 表示未设置
     */
    long initialDelayMillis() default -1L;

    /**
     * @return 退避倍增系数；{@code -1} 表示未设置
     */
    double multiplier() default -1D;

    /**
     * @return 最大重试延迟毫秒数；{@code -1} 表示未设置
     */
    long maxDelayMillis() default -1L;
}
