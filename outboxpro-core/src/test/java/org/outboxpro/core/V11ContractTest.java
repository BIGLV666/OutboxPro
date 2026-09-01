package org.outboxpro.core;

import org.junit.jupiter.api.Test;
import org.outboxpro.core.annotation.NonRetryable;
import org.outboxpro.core.annotation.RetryPolicySpec;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.event.EventRegistry;
import org.outboxpro.core.exception.EventConfigurationException;
import org.outboxpro.core.exception.NonRetryableExceptions;
import org.outboxpro.core.retry.RetryPolicies;
import org.outboxpro.core.retry.RetryPolicy;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * V1.1 公共契约单元测试：类型安全注册表反查、注解式重试策略解析与 @NonRetryable 判定。
 */
class V11ContractTest {

    /** 载荷类型唯一时反查成功；多个事件共用载荷类型时提示改用 eventType 重载。 */
    @Test
    void requireByPayloadTypeResolvesAndRejectsAmbiguity() {
        EventRegistry registry = new EventRegistry();
        registry.register(new EventDefinition<>("a.created", "v1", PayloadA.class,
                new org.outboxpro.core.event.EventRoute("ex", "a.created")));

        assertThat(registry.requireByPayloadType(PayloadA.class).getEventType()).isEqualTo("a.created");
        assertThatThrownBy(() -> registry.requireByPayloadType(PayloadB.class))
                .isInstanceOf(EventConfigurationException.class)
                .hasMessageContaining("No event registered");

        registry.register(new EventDefinition<>("b.created", "v1", PayloadA.class,
                new org.outboxpro.core.event.EventRoute("ex", "b.created")));
        assertThatThrownBy(() -> registry.requireByPayloadType(PayloadA.class))
                .isInstanceOf(EventConfigurationException.class)
                .hasMessageContaining("multiple events");
    }

    /** 全部哨兵值返回 null（沿用默认策略）；部分字段显式设置时未设置字段回退默认值。 */
    @Test
    void retryPolicySpecResolution() {
        RetryPolicySpec unset = defaultSpec();
        assertThat(RetryPolicies.fromSpec(unset)).isNull();

        RetryPolicySpec partial = new RetryPolicySpec() {
            @Override public boolean enabled() { return false; }
            @Override public int maxAttempts() { return 2; }
            @Override public long initialDelayMillis() { return RetryPolicySpec.UNSET; }
            @Override public double multiplier() { return RetryPolicySpec.UNSET; }
            @Override public long maxDelayMillis() { return RetryPolicySpec.UNSET; }
            @Override public Class<? extends java.lang.annotation.Annotation> annotationType() { return RetryPolicySpec.class; }
        };
        RetryPolicy resolved = RetryPolicies.fromSpec(partial);
        assertThat(resolved).isNotNull();
        assertThat(resolved.enabled()).isFalse();
        assertThat(resolved.maxAttempts()).isEqualTo(2);
        assertThat(resolved.initialDelay()).isEqualTo(Duration.ofSeconds(1));
        assertThat(resolved.multiplier()).isEqualTo(2);
        assertThat(resolved.maxDelay()).isEqualTo(Duration.ofMinutes(5));
    }

    /** @NonRetryable 标注：本类、父类与因果链包装均可识别。 */
    @Test
    void nonRetryableAnnotationDetection() {
        assertThat(NonRetryableExceptions.isNonRetryable(new MarkedException("x"))).isTrue();
        assertThat(NonRetryableExceptions.isNonRetryable(new MarkedSubclass("x"))).isTrue();
        assertThat(NonRetryableExceptions.isNonRetryable(new RuntimeException("wrapper", new MarkedException("cause")))).isTrue();
        assertThat(NonRetryableExceptions.isNonRetryable(new IllegalStateException("plain"))).isFalse();
        assertThat(NonRetryableExceptions.isNonRetryable(null)).isFalse();
    }

    /** 未设置任何字段的注解实例。 */
    private RetryPolicySpec defaultSpec() {
        return new RetryPolicySpec() {
            @Override public boolean enabled() { return true; }
            @Override public int maxAttempts() { return (int) RetryPolicySpec.UNSET; }
            @Override public long initialDelayMillis() { return RetryPolicySpec.UNSET; }
            @Override public double multiplier() { return RetryPolicySpec.UNSET; }
            @Override public long maxDelayMillis() { return RetryPolicySpec.UNSET; }
            @Override public Class<? extends java.lang.annotation.Annotation> annotationType() { return RetryPolicySpec.class; }
        };
    }

    // 测试载荷类型
    static class PayloadA { }
    static class PayloadB { }

    // 标注与未标注的测试异常
    @NonRetryable
    static class MarkedException extends RuntimeException {
        MarkedException(String message) { super(message); }
    }

    static class MarkedSubclass extends MarkedException {
        MarkedSubclass(String message) { super(message); }
    }
}
