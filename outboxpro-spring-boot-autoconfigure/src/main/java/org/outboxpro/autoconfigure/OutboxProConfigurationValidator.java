package org.outboxpro.autoconfigure;

import java.time.Duration;

/**
 * OutboxPro 全局配置的启动校验器。
 *
 * <p>在 Spring 上下文启动阶段对生产端、消费端与重试配置做范围与约束校验，
 * 非法配置直接导致启动失败（fail-fast），错误信息包含属性名与期望格式。
 * 校验器不依赖 DataSource、RabbitTemplate 或死信仓储，
 * 确保配置错误不会因为条件装配而被静默忽略。</p>
 */
public final class OutboxProConfigurationValidator {

    /**
     * 校验全局配置，非法时抛出携带属性名的异常。
     *
     * @param properties OutboxPro 配置
     * @throws IllegalStateException 任一配置超出合法范围或违反约束时抛出
     */
    public OutboxProConfigurationValidator(OutboxProProperties properties) {
        if (properties == null) {
            throw new IllegalStateException("outboxpro configuration must not be null");
        }
        if (properties.getProducerName() == null || properties.getProducerName().isBlank()) {
            throw new IllegalStateException("outboxpro.producer-name must not be blank");
        }
        validateProducer(properties);
        validateConsumer(properties);
        validateRetry(properties);
        validateObservability(properties);
        validateRetention(properties);
    }

    /** 校验生产端配置范围。 */
    private void validateProducer(OutboxProProperties properties) {
        OutboxProProperties.Producer producer = properties.getProducer();
        if (producer == null) {
            throw new IllegalStateException("outboxpro.producer configuration must not be null");
        }
        if (producer.getBatchSize() <= 0) {
            throw new IllegalStateException("outboxpro.producer.batch-size must be positive");
        }
        if (producer.getPollInterval() == null || producer.getPollInterval().isZero() || producer.getPollInterval().isNegative()) {
            throw new IllegalStateException("outboxpro.producer.poll-interval must be a positive duration, e.g. 1s");
        }
        if (producer.getClaimTimeout() == null || producer.getClaimTimeout().isZero() || producer.getClaimTimeout().isNegative()) {
            throw new IllegalStateException("outboxpro.producer.claim-timeout must be a positive duration, e.g. 60s");
        }
        if (producer.getConfirmTimeout() == null || producer.getConfirmTimeout().isZero() || producer.getConfirmTimeout().isNegative()) {
            throw new IllegalStateException("outboxpro.producer.confirm-timeout must be a positive duration, e.g. 10s");
        }
    }

    /**
     * 校验消费端配置范围，并强制 Inbox 幂等开启。
     *
     * <p>V1 的可靠性语义建立在 {@code consumerName + eventId} Inbox 幂等之上，
     * 关闭幂等会破坏"业务只执行一次"的承诺，因此显式关闭属于配置错误。</p>
     */
    private void validateConsumer(OutboxProProperties properties) {
        OutboxProProperties.Consumer consumer = properties.getConsumer();
        if (consumer == null) {
            throw new IllegalStateException("outboxpro.consumer configuration must not be null");
        }
        if (!consumer.isIdempotencyEnabled()) {
            throw new IllegalStateException(
                    "outboxpro.consumer.idempotency-enabled must be true in V1: "
                            + "at-least-once delivery requires the Inbox idempotency guarantee");
        }
        if (consumer.getConcurrency() <= 0) {
            throw new IllegalStateException("outboxpro.consumer.concurrency must be positive");
        }
        if (consumer.getPrefetch() <= 0) {
            throw new IllegalStateException("outboxpro.consumer.prefetch must be positive");
        }
        validateDuration(consumer.getRedeliveryDelay(), "outboxpro.consumer.redelivery-delay", true, Duration.ofSeconds(60));
        // RECEIVED 重开超时过小会让并发重复投递在正常 Handler 执行期间互相重开，造成双重执行。
        validateDuration(consumer.getInboxReceivedStaleTimeout(), "outboxpro.consumer.inbox-received-stale-timeout",
                false, null);
        if (consumer.getInboxReceivedStaleTimeout().compareTo(Duration.ofMinutes(1)) < 0) {
            throw new IllegalStateException(
                    "outboxpro.consumer.inbox-received-stale-timeout must be at least 1m to avoid re-opening "
                            + "RECEIVED inbox rows while a concurrent delivery is still executing");
        }
        if (consumer.getMaxRetryQueueCount() < 1) {
            throw new IllegalStateException(
                    "outboxpro.consumer.max-retry-queue-count must be >= 1 (total retry queues across "
                            + "all subscriptions; reduce per-binding maxAttempts or disable retry instead)");
        }
    }

    /**
     * 校验消费端新增的时长配置。
     *
     * @param value 配置值
     * @param name 配置名
     * @param allowZero 是否允许为零
     * @param max 上限；null 表示不限制
     */
    private void validateDuration(Duration value, String name, boolean allowZero, Duration max) {
        if (value == null || value.isNegative() || (!allowZero && value.isZero())) {
            throw new IllegalStateException(name + " must be a positive duration"
                    + (allowZero ? " (0 allowed)" : ""));
        }
        if (max != null && value.compareTo(max) > 0) {
            throw new IllegalStateException(name + " must not exceed " + max);
        }
    }

    /** 校验历史数据保留策略配置范围。 */
    private void validateRetention(OutboxProProperties properties) {
        OutboxProProperties.Retention retention = properties.getRetention();
        if (retention == null) {
            throw new IllegalStateException("outboxpro.retention configuration must not be null");
        }
        if (!retention.isEnabled()) {
            return;
        }
        if (retention.getOutboxSent() == null || retention.getOutboxSent().isNegative()
                || retention.getOutboxSent().isZero()) {
            throw new IllegalStateException("outboxpro.retention.outbox-sent must be a positive duration, e.g. 14d");
        }
        if (retention.getInboxSuccess() == null || retention.getInboxSuccess().isNegative()
                || retention.getInboxSuccess().isZero()) {
            throw new IllegalStateException("outboxpro.retention.inbox-success must be a positive duration, e.g. 30d");
        }
        if (retention.getMessageLog() == null || retention.getMessageLog().isNegative()
                || retention.getMessageLog().isZero()) {
            throw new IllegalStateException("outboxpro.retention.message-log must be a positive duration, e.g. 14d");
        }
        if (retention.getDeadLetterReplayed() == null || retention.getDeadLetterReplayed().isNegative()
                || retention.getDeadLetterReplayed().isZero()) {
            throw new IllegalStateException(
                    "outboxpro.retention.dead-letter-replayed must be a positive duration, e.g. 30d");
        }
        if (retention.getBatchSize() <= 0) {
            throw new IllegalStateException("outboxpro.retention.batch-size must be positive");
        }
        if (retention.getMaxRowsPerCycle() <= 0) {
            throw new IllegalStateException("outboxpro.retention.max-rows-per-cycle must be positive");
        }
    }

    /** 校验重试配置范围（与 RetryPolicy 构造约束一致）。 */
    private void validateRetry(OutboxProProperties properties) {
        OutboxProProperties.Retry retry = properties.getRetry();
        if (retry == null) {
            throw new IllegalStateException("outboxpro.retry configuration must not be null");
        }
        if (retry.getMaxAttempts() < 1) {
            throw new IllegalStateException("outboxpro.retry.max-attempts must be >= 1");
        }
        if (retry.getInitialDelay() == null || retry.getInitialDelay().isNegative()) {
            throw new IllegalStateException("outboxpro.retry.initial-delay must be a non-negative duration, e.g. 1s");
        }
        if (retry.getMultiplier() < 1) {
            throw new IllegalStateException("outboxpro.retry.multiplier must be >= 1");
        }
        if (retry.getMaxDelay() == null || retry.getMaxDelay().isNegative()) {
            throw new IllegalStateException("outboxpro.retry.max-delay must be a non-negative duration, e.g. 5m");
        }
    }

    /** 校验观测配置：消息日志 Sink 类型与数据库 Sink 的范围约束。 */
    private void validateObservability(OutboxProProperties properties) {
        OutboxProProperties.Observability observability = properties.getObservability();
        if (observability == null) {
            throw new IllegalStateException("outboxpro.observability configuration must not be null");
        }
        String sink = observability.getMessageLogSink();
        if (!"slf4j".equals(sink) && !"database".equals(sink)) {
            throw new IllegalStateException(
                    "outboxpro.observability.message-log-sink must be one of [slf4j, database]");
        }
        if ("database".equals(sink)) {
            OutboxProProperties.Observability.DbSink dbSink = observability.getDbSink();
            if (dbSink == null || dbSink.getBatchSize() <= 0) {
                throw new IllegalStateException("outboxpro.observability.db-sink.batch-size must be positive");
            }
            if (dbSink.getQueueCapacity() <= 0) {
                throw new IllegalStateException("outboxpro.observability.db-sink.queue-capacity must be positive");
            }
            if (dbSink.getFlushIntervalMillis() <= 0) {
                throw new IllegalStateException(
                        "outboxpro.observability.db-sink.flush-interval-milliseconds must be positive");
            }
        }
    }
}
