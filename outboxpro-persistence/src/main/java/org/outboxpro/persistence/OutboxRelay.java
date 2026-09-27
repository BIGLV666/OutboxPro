package org.outboxpro.persistence;

import org.outboxpro.core.metrics.OutboxMetrics;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.spi.observability.MessageLogSink;
import org.outboxpro.spi.observability.MessageStage;
import org.outboxpro.spi.observability.MessageStatus;
import org.outboxpro.spi.observability.MessageTraceRecord;
import org.outboxpro.spi.persistence.OutboxRecord;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.outboxpro.spi.transport.MessagePublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Outbox 投递中继，负责批量认领消息、事务外发布和状态更新。
 * 认领阶段使用短事务，RabbitMQ 网络调用发生在数据库事务提交之后。
 *
 * <p>发布阶段通过 {@link MessagePublisher#publishAll(List)} 一次交出整批记录，
 * 支持管道化 Confirm 的实现可以把 N 次串行往返压缩为一次写出加批量等待；
 * 未覆写的实现自动退化为逐条发布，行为与旧版本一致。</p>
 */
public final class OutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository repository;
    private final MessagePublisher publisher;
    private final RetryPolicy retryPolicy;
    private final int batchSize;
    private final Duration claimTimeout;
    private final OutboxMetrics metrics;
    private final MessageLogSink logSink;
    private final String owner = UUID.randomUUID().toString();

    /**
     * 创建 Relay（无指标上报、无消息日志）。
     */
    public OutboxRelay(OutboxRepository repository, MessagePublisher publisher, RetryPolicy retryPolicy,
                       int batchSize, Duration claimTimeout) {
        this(repository, publisher, retryPolicy, batchSize, claimTimeout, OutboxMetrics.NOOP, null);
    }

    /**
     * 创建 Relay（无消息日志）。
     *
     * @param repository Outbox 持久化实现
     * @param publisher MQ 发布实现
     * @param retryPolicy 生产端失败重试策略
     * @param batchSize 单次认领的最大消息数
     * @param claimTimeout 认领租约时长，超过后允许其他实例恢复
     * @param metrics 指标上报门面
     */
    public OutboxRelay(OutboxRepository repository, MessagePublisher publisher, RetryPolicy retryPolicy,
                       int batchSize, Duration claimTimeout, OutboxMetrics metrics) {
        this(repository, publisher, retryPolicy, batchSize, claimTimeout, metrics, null);
    }

    /**
     * 创建 Relay。
     *
     * @param repository Outbox 持久化实现
     * @param publisher MQ 发布实现
     * @param retryPolicy 生产端失败重试策略
     * @param batchSize 单次认领的最大消息数
     * @param claimTimeout 认领租约时长，超过后允许其他实例恢复
     * @param metrics 指标上报门面
     * @param logSink 消息生命周期日志 Sink，可为空；日志是旁路能力，失败绝不影响投递
     */
    public OutboxRelay(OutboxRepository repository, MessagePublisher publisher, RetryPolicy retryPolicy,
                       int batchSize, Duration claimTimeout, OutboxMetrics metrics, MessageLogSink logSink) {
        this.repository = repository;
        this.publisher = publisher;
        this.retryPolicy = retryPolicy;
        this.batchSize = batchSize;
        this.claimTimeout = claimTimeout;
        this.metrics = metrics == null ? OutboxMetrics.NOOP : metrics;
        this.logSink = logSink;
    }

    /**
     * 执行一次 Outbox 认领、发布和状态更新循环。
     * 发布成功标记为 SENT；可重试异常进入 RETRY_WAITING；超过次数进入 DEAD。
     *
     * <p>所有状态迁移都走带返回值的 transition 系列方法：认领租约可能已被其他实例的
     * 过期恢复复位，迁移返回 false 代表"租约丢失"，此时消息语义仍是至少一次
     * （记录会被其他实例重新发布），但必须打点告警暴露 claimTimeout 配置问题。</p>
     */
    public void relayOnce() {
        Instant now = Instant.now();
        repository.recoverExpiredClaims(now);
        List<OutboxRecord> records = repository.claimBatch(owner, batchSize, now, now.plus(claimTimeout));
        if (!records.isEmpty()) {
            metrics.relayClaimed(records.size());
        }
        if (records.isEmpty()) {
            return;
        }
        for (OutboxRecord record : records) {
            metrics.publishAttempt(record.eventType(), record.producer());
        }

        List<MessagePublisher.PublishOutcome> outcomes = publisher.publishAll(records);
        if (outcomes.size() != records.size()) {
            // 实现契约破坏：无法把结果对应回记录。让整批保持 PROCESSING，由租约过期机制兜底恢复。
            throw new IllegalStateException("MessagePublisher.publishAll returned " + outcomes.size()
                    + " outcomes for " + records.size() + " records");
        }

        for (int i = 0; i < records.size(); i++) {
            OutboxRecord record = records.get(i);
            MessagePublisher.PublishOutcome outcome = outcomes.get(i);
            int attempt = record.attemptCount() + 1;
            if (outcome.success()) {
                metrics.publishSuccess(record.eventType(), record.producer());
                if (!repository.transitionToSent(record.id(), owner, Instant.now())) {
                    reportLeaseLost(record);
                }
                logSink(record, MessageStatus.SUCCESS, attempt, null);
            } else {
                metrics.publishFailure(record.eventType(), record.producer());
                if (retryPolicy.enabled() && attempt < retryPolicy.maxAttempts()) {
                    boolean moved = repository.transitionToRetryWaiting(record.id(), owner, attempt,
                            Instant.now().plus(retryPolicy.delayForAttempt(attempt)),
                            outcome.errorType(), outcome.errorMessage());
                    if (!moved) {
                        reportLeaseLost(record);
                    }
                    logSink(record, MessageStatus.RETRYING, attempt, outcome);
                } else {
                    if (!repository.transitionToDead(record.id(), owner, attempt,
                            outcome.errorType(), outcome.errorMessage())) {
                        reportLeaseLost(record);
                    }
                    logSink(record, MessageStatus.DEAD, attempt, outcome);
                }
            }
        }
    }

    /** 租约丢失说明 claimTimeout 短于真实发布耗时，或实例中途停顿，必须让运维看到。 */
    private void reportLeaseLost(OutboxRecord record) {
        metrics.relayLeaseLost(record.eventType(), record.producer());
        log.warn("Outbox claim lease was lost before transition for event {} (id={}); "
                        + "the record has been recovered by another instance and may be published again. "
                        + "Consider increasing outboxpro.producer.claim-timeout or reducing producer.batch-size",
                record.eventId(), record.id());
    }

    /** 生产端生命周期日志：旁路能力，任何异常只降级不传播。 */
    private void logSink(OutboxRecord record, MessageStatus status, int attempt,
                         MessagePublisher.PublishOutcome outcome) {
        if (logSink == null) {
            return;
        }
        try {
            logSink.append(new MessageTraceRecord(
                    record.eventId(), null, record.eventType(), record.traceId(),
                    record.correlationId(), record.causationId(), record.producer(),
                    null, record.exchangeName(), null, record.routingKey(),
                    MessageStage.PUBLISH, status, attempt, null,
                    outcome == null ? null : outcome.errorType(),
                    outcome == null ? null : outcome.errorMessage(),
                    Instant.now()));
        } catch (RuntimeException ignored) {
            // 日志 Sink 是旁路能力，绝不允许它改变投递结果。
        }
    }
}
