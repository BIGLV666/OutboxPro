package org.outboxpro.persistence;

import org.outboxpro.core.metrics.OutboxMetrics;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.spi.persistence.OutboxRecord;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.outboxpro.spi.transport.MessagePublisher;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Outbox 投递中继，负责批量认领消息、事务外发布和状态更新。
 * 认领阶段使用短事务，RabbitMQ 网络调用发生在数据库事务提交之后。
 */
public final class OutboxRelay {
    private final OutboxRepository repository;
    private final MessagePublisher publisher;
    private final RetryPolicy retryPolicy;
    private final int batchSize;
    private final Duration claimTimeout;
    private final OutboxMetrics metrics;
    private final String owner = UUID.randomUUID().toString();

    /**
     * 创建 Relay（无指标上报）。
     */
    public OutboxRelay(OutboxRepository repository, MessagePublisher publisher, RetryPolicy retryPolicy,
                       int batchSize, Duration claimTimeout) {
        this(repository, publisher, retryPolicy, batchSize, claimTimeout, OutboxMetrics.NOOP);
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
     */
    public OutboxRelay(OutboxRepository repository, MessagePublisher publisher, RetryPolicy retryPolicy,
                       int batchSize, Duration claimTimeout, OutboxMetrics metrics) {
        this.repository = repository;
        this.publisher = publisher;
        this.retryPolicy = retryPolicy;
        this.batchSize = batchSize;
        this.claimTimeout = claimTimeout;
        this.metrics = metrics == null ? OutboxMetrics.NOOP : metrics;
    }

    /**
     * 执行一次 Outbox 认领、发布和状态更新循环。
     * 发布成功标记为 SENT；可重试异常进入 RETRY_WAITING；超过次数进入 DEAD。
     */
    public void relayOnce() {
        Instant now = Instant.now();
        repository.recoverExpiredClaims(now);
        List<OutboxRecord> records = repository.claimBatch(owner, batchSize, now, now.plus(claimTimeout));
        if (!records.isEmpty()) {
            metrics.relayClaimed(records.size());
        }
        for (OutboxRecord record : records) {
            int attempt = record.attemptCount() + 1;
            metrics.publishAttempt(record.eventType(), record.producer());
            try {
                publisher.publish(record);
                repository.markSent(record.id(), owner, Instant.now());
                metrics.publishSuccess(record.eventType(), record.producer());
            } catch (RuntimeException error) {
                metrics.publishFailure(record.eventType(), record.producer());
                if (retryPolicy.enabled() && attempt < retryPolicy.maxAttempts()) {
                    repository.markRetryWaiting(record.id(), owner, attempt,
                            Instant.now().plus(retryPolicy.delayForAttempt(attempt)),
                            error.getClass().getName(), error.getMessage());
                } else {
                    repository.markDead(record.id(), owner, attempt,
                            error.getClass().getName(), error.getMessage());
                }
            }
        }
    }
}
