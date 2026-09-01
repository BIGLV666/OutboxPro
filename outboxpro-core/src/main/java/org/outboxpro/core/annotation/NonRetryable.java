package org.outboxpro.core.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标注在异常类上，声明该类异常代表「不可重试」的业务失败。
 *
 * <p>Handler 抛出（或因果链中包含）被本注解标注的异常时，框架跳过重试直接进入死信流程，
 * 等价于抛出 {@link org.outboxpro.core.exception.NonRetryableEventException}，
 * 但允许业务保留自定义异常类型（例如 {@code InsufficientBalanceException}）。
 * 注解沿父类层次生效：标注在业务异常基类上即可覆盖全部子类。</p>
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface NonRetryable {
}
