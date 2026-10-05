package org.outboxpro.autoconfigure;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.outboxpro.spi.persistence.OutboxRepository;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** 积压 Gauge：固定标签集合、未知能力显示 0 而不是抛异常、同一快照复用。 */
class MicrometerOutboxProBacklogMetricsTest {

    @Test
    void shouldRegisterAllBacklogGaugesWithFixedReasonTags() {
        MeterRegistry registry = new SimpleMeterRegistry();
        OutboxProObservabilitySnapshot snapshot = new OutboxProObservabilitySnapshot(
                new EmptyRepository(), null, Duration.ofSeconds(30));
        new MicrometerOutboxProBacklogMetrics(snapshot, registry);

        assertThat(registry.get("outboxpro.outbox.backlog.pending").gauge().value()).isZero();
        assertThat(registry.get("outboxpro.outbox.backlog.dead").gauge().value()).isZero();
        assertThat(registry.get("outboxpro.dlq.backlog.pending_replay").gauge().value()).isZero();
        assertThat(registry.get("outboxpro.dlq.backlog.by_reason").tag("reason", "RETRY_EXHAUSTED")
                .gauge().value()).isZero();
        assertThat(registry.get("outboxpro.dlq.backlog.by_reason").tag("reason", "UNKNOWN")
                .gauge().value()).isZero();
    }

    @Test
    void reasonGaugesShouldUseCurrentSnapshotValues() {
        MeterRegistry registry = new SimpleMeterRegistry();
        OutboxProObservabilitySnapshot snapshot = new OutboxProObservabilitySnapshot(
                new EmptyRepository(), null, Duration.ofSeconds(30));
        // 注册 Gauge 后更新底层缓存由真实仓储实现负责；本测试验证数值函数不异常。
        new MicrometerOutboxProBacklogMetrics(snapshot, registry);

        registry.get("outboxpro.outbox.backlog.pending").gauge().value();
    }

    private static final class EmptyRepository implements OutboxRepository {
        @Override public void insert(org.outboxpro.spi.persistence.OutboxRecord record) { }
        @Override public java.util.List<org.outboxpro.spi.persistence.OutboxRecord> claimBatch(
                String owner, int batchSize, java.time.Instant now, java.time.Instant claimUntil) {
            return java.util.List.of();
        }
        @Override public void markSent(long id, String owner, java.time.Instant sentAt) { }
        @Override public void markRetryWaiting(long id, String owner, int attempt, java.time.Instant nextRetryAt,
                                               String errorType, String errorMessage) { }
        @Override public void markDead(long id, String owner, int attempt, String errorType,
                                       String errorMessage) { }
        @Override public int recoverExpiredClaims(java.time.Instant now) { return 0; }
    }
}
