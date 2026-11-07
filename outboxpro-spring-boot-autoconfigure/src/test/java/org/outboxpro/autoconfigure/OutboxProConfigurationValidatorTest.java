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

    /** redelivery-delay 允许为 0，但不允许为负或超过 60s。 */
    @Test
    void redeliveryDelayBounds() {
        OutboxProProperties properties = validProperties();
        properties.getConsumer().setRedeliveryDelay(Duration.ofSeconds(90));
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.consumer.redelivery-delay");
        properties.getConsumer().setRedeliveryDelay(Duration.ofMillis(-1));
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.consumer.redelivery-delay");
        properties.getConsumer().setRedeliveryDelay(Duration.ZERO);
        assertThatCode(() -> new OutboxProConfigurationValidator(properties)).doesNotThrowAnyException();
    }

    /** RECEIVED 孤儿重开超时过小会在并发执行期间互相重开，必须拒绝。 */
    @Test
    void inboxReceivedStaleTimeoutMustBeAtLeastOneMinute() {
        OutboxProProperties properties = validProperties();
        properties.getConsumer().setInboxReceivedStaleTimeout(Duration.ofSeconds(30));
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.consumer.inbox-received-stale-timeout");
    }

    /** Retry Queue 总量上限必须为正数。 */
    @Test
    void maxRetryQueueCountMustBePositive() {
        OutboxProProperties properties = validProperties();
        properties.getConsumer().setMaxRetryQueueCount(0);
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.consumer.max-retry-queue-count");
    }

    /** 观测快照 TTL 不能为 0，避免监控抓取把聚合查询放大成数据库压力。 */
    @Test
    void metricsSnapshotTtlMustBePositive() {
        OutboxProProperties properties = validProperties();
        properties.getMetrics().setSnapshotTtl(Duration.ZERO);
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.metrics.snapshot-ttl");
    }

    /** 健康阈值可以为 0，用于用户显式关闭阈值告警语义。 */
    @Test
    void metricsThresholdsMayBeZeroButNotNegative() {
        OutboxProProperties properties = validProperties();
        properties.getMetrics().getHealth().setOutboxBacklogThreshold(0);
        properties.getMetrics().getHealth().setOutboxDeadThreshold(0);
        properties.getMetrics().getHealth().setDlqPendingThreshold(0);
        assertThatCode(() -> new OutboxProConfigurationValidator(properties)).doesNotThrowAnyException();

        properties.getMetrics().getHealth().setOutboxDeadThreshold(-1);
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.metrics.health.outbox-dead-threshold");
    }

    /** V1.4 新增生产端配置：workers、空闲退避上限与批次上限都要在启动期校验。 */
    @Test
    void producerCapacityConfigurationBounds() {
        OutboxProProperties noWorkers = validProperties();
        noWorkers.getProducer().setWorkers(0);
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(noWorkers))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.producer.workers");

        OutboxProProperties tooManyWorkers = validProperties();
        tooManyWorkers.getProducer().setWorkers(65);
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(tooManyWorkers))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.producer.workers");

        OutboxProProperties zeroBackoff = validProperties();
        zeroBackoff.getProducer().setIdleBackoffMax(Duration.ZERO);
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(zeroBackoff))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.producer.idle-backoff-max");

        OutboxProProperties oversizedBatch = validProperties();
        oversizedBatch.getProducer().setBatchSize(10_001);
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(oversizedBatch))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.producer.batch-size");
    }

    /** Confirm 等待必须短于认领租约，否则租约过期会造成重复发布。 */
    @Test
    void confirmTimeoutMustBeShorterThanClaimTimeout() {
        OutboxProProperties properties = validProperties();
        properties.getProducer().setConfirmTimeout(Duration.ofSeconds(60));
        properties.getProducer().setClaimTimeout(Duration.ofSeconds(60));
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.producer.confirm-timeout");

        properties.getProducer().setConfirmTimeout(Duration.ofSeconds(59));
        assertThatCode(() -> new OutboxProConfigurationValidator(properties)).doesNotThrowAnyException();
    }

    /** 保留策略各时长必须为正，批大小与单轮上限必须为正。 */
    @Test
    void retentionConfigurationBounds() {
        OutboxProProperties zeroSent = validProperties();
        zeroSent.getRetention().setOutboxSent(Duration.ZERO);
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(zeroSent))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.retention.outbox-sent");

        OutboxProProperties zeroBatch = validProperties();
        zeroBatch.getRetention().setBatchSize(0);
        assertThatThrownBy(() -> new OutboxProConfigurationValidator(zeroBatch))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxpro.retention.batch-size");

        OutboxProProperties disabled = validProperties();
        disabled.getRetention().setEnabled(false);
        assertThatCode(() -> new OutboxProConfigurationValidator(disabled)).doesNotThrowAnyException();
    }
}
