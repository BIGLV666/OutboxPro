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

    /** 重复注册不能污染载荷索引，已返回的快照保持稳定且不可修改。 */
    @Test
    void registrySnapshotsRemainImmutableAndRejectedRegistrationDoesNotChangeIndex() {
        EventRegistry registry = new EventRegistry();
        var first = new EventDefinition<>("a", "v1", PayloadA.class,
                new org.outboxpro.core.event.EventRoute("ex", "a"));
        registry.register(first);
        var snapshot = registry.definitions();
        assertThatThrownBy(() -> registry.register(new EventDefinition<>("a", "v1", PayloadB.class,
                new org.outboxpro.core.event.EventRoute("ex", "b"))))
                .isInstanceOf(EventConfigurationException.class);
        assertThat(registry.requireByPayloadType(PayloadA.class)).isSameAs(first);
        assertThatThrownBy(() -> registry.requireByPayloadType(PayloadB.class))
                .isInstanceOf(EventConfigurationException.class);
        registry.register(new EventDefinition<>("b", "v1", PayloadB.class,
                new org.outboxpro.core.event.EventRoute("ex", "b")));
        assertThat(snapshot).containsOnlyKeys("a");
        assertThatThrownBy(snapshot::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThat(registry.requireByPayloadType(PayloadB.class).getEventType()).isEqualTo("b");
    }

    /** 多线程读写注册表时，只能读到完整发布的不可变快照。 */
    @Test
    void concurrentRegistrationPublishesCompleteSnapshots() throws Exception {
        EventRegistry registry = new EventRegistry();
        var start = new java.util.concurrent.CountDownLatch(1);
        try (var executor = java.util.concurrent.Executors.newFixedThreadPool(2)) {
            var writer = executor.submit(() -> {
                start.await();
                for (int i = 0; i < 100; i++) {
                    registry.register(new EventDefinition<>("event-" + i, "v1", PayloadA.class,
                            new org.outboxpro.core.event.EventRoute("ex", "key")));
                }
                return null;
            });
            var reader = executor.submit(() -> {
                start.await();
                for (int i = 0; i < 1000; i++) {
                    for (var entry : registry.definitions().entrySet()) {
                        assertThat(registry.require(entry.getKey())).isSameAs(entry.getValue());
                    }
                }
                return null;
            });
            start.countDown();
            writer.get(10, java.util.concurrent.TimeUnit.SECONDS);
            reader.get(10, java.util.concurrent.TimeUnit.SECONDS);
        }
        assertThat(registry.definitions()).hasSize(100);
        assertThatThrownBy(() -> registry.requireByPayloadType(PayloadA.class))
                .isInstanceOf(EventConfigurationException.class).hasMessageContaining("multiple events");
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

    /** 只关闭 enabled 时必须生成覆盖策略，而不是继承全局重试。 */
    @Test
    void disabledOnlyRetryPolicyOverridesDefaults() {
        RetryPolicy resolved = RetryPolicies.fromSpec(DisabledOnly.class
                .getAnnotation(org.outboxpro.core.annotation.OutboxHandler.class).retry());
        assertThat(resolved).isNotNull();
        assertThat(resolved.enabled()).isFalse();
        assertThat(resolved.maxAttempts()).isEqualTo(RetryPolicy.defaults().maxAttempts());
    }

    @org.outboxpro.core.annotation.OutboxHandler(event = PayloadA.class, queue = "test",
            retry = @RetryPolicySpec(enabled = false))
    static class DisabledOnly { }

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
