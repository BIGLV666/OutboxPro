package org.outboxpro.autoconfigure;

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
