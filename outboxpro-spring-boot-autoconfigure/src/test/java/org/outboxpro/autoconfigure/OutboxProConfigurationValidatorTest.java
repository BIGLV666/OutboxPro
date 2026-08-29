package org.outboxpro.autoconfigure;

import org.junit.jupiter.api.Test;
import org.outboxpro.core.retry.RetryPolicy;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link OutboxProConfigurationValidator} 单元测试：正常配置通过，
 * 各属性越界时 fail-fast 且错误信息包含属性名。
 */
class OutboxProConfigurationValidatorTest {

    /** 构造一份合法的默认配置。 */
    private OutboxProProperties validProperties() {
        OutboxProProperties properties = new OutboxProProperties();
        properties.setProducerName("order-service");
        return properties;
    }

    @Test
    void defaultConfigurationMustPass() {
        assertThatCode(() -> new OutboxProConfigurationValidator(validProperties()))
                .doesNotThrowAnyException();
    }

    @Test
    void blankProducerNameMustFail() {
        OutboxProProperties properties = validProperties();
        properties.setProducerName(" ");
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.producer-name");
    }

    @Test
    void nonPositiveBatchSizeMustFail() {
        OutboxProProperties properties = validProperties();
        properties.getProducer().setBatchSize(0);
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.producer.batch-size");
    }

    @Test
    void nonPositivePollIntervalMustFail() {
        OutboxProProperties properties = validProperties();
        properties.getProducer().setPollInterval(Duration.ZERO);
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.producer.poll-interval");
    }

    @Test
    void nonPositiveClaimTimeoutMustFail() {
        OutboxProProperties properties = validProperties();
        properties.getProducer().setClaimTimeout(Duration.ofSeconds(-1));
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.producer.claim-timeout");
    }

    @Test
    void disabledIdempotencyMustFailInV1() {
        OutboxProProperties properties = validProperties();
        properties.getConsumer().setIdempotencyEnabled(false);
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.consumer.idempotency-enabled");
    }

    @Test
    void nonPositiveConcurrencyMustFail() {
        OutboxProProperties properties = validProperties();
        properties.getConsumer().setConcurrency(0);
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.consumer.concurrency");
    }

    @Test
    void nonPositivePrefetchMustFail() {
        OutboxProProperties properties = validProperties();
        properties.getConsumer().setPrefetch(-1);
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.consumer.prefetch");
    }

    @Test
    void invalidRetryAttemptsMustFail() {
        OutboxProProperties properties = validProperties();
        properties.getRetry().setMaxAttempts(0);
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.retry.max-attempts");
    }

    @Test
    void multiplierBelowOneMustFail() {
        OutboxProProperties properties = validProperties();
        properties.getRetry().setMultiplier(0.5);
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.retry.multiplier");
    }

    @Test
    void negativeInitialDelayMustFail() {
        OutboxProProperties properties = validProperties();
        properties.getRetry().setInitialDelay(Duration.ofMillis(-10));
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.retry.initial-delay");
    }

    @Test
    void negativeMaxDelayMustFail() {
        OutboxProProperties properties = validProperties();
        properties.getRetry().setMaxDelay(Duration.ofSeconds(-5));
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.retry.max-delay");
    }

    /** RetryPolicy 自身构造约束与校验器一致，防止两处语义漂移。 */
    @Test
    void retryPolicyConstraintsStayAligned() {
        assertThatCode(() -> new RetryPolicy(true, 5, Duration.ofSeconds(1), 2, Duration.ofMinutes(5)))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> new RetryPolicy(true, 0, Duration.ofSeconds(1), 2, Duration.ofMinutes(5)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
