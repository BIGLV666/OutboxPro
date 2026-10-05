package org.outboxpro.autoconfigure;

import org.junit.jupiter.api.Test;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** 积压健康检查：终态死信 DOWN，待处理积压 OUT_OF_SERVICE，普通积压与未知能力不误报。 */
class OutboxProHealthIndicatorTest {

    @Test
    void normalBacklogShouldBeUp() {
        Health health = indicator(1000, 1, 100, Map.of(
                "PENDING", 10L, "PROCESSING", 5L, "RETRY_WAITING", 0L, "DEAD", 0L), Map.of()).health();
        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("outboxPending", 10L);
    }

    @Test
    void outboxBacklogThresholdShouldBeOutOfService() {
        Health health = indicator(15, 1, 100, Map.of(
                "PENDING", 10L, "PROCESSING", 0L, "RETRY_WAITING", 5L, "DEAD", 0L), Map.of()).health();
        assertThat(health.getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
    }

    @Test
    void deadOutboxShouldBeDown() {
        Health health = indicator(1000, 1, 100, Map.of(
                "PENDING", 0L, "PROCESSING", 0L, "RETRY_WAITING", 0L, "DEAD", 1L), Map.of()).health();
        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    }

    @Test
    void pendingDeadLettersShouldBeDownAtThreshold() {
        Health health = indicator(1000, 1, 2, Map.of(
                "PENDING", 0L, "PROCESSING", 0L, "RETRY_WAITING", 0L, "DEAD", 0L),
                Map.of("RETRY_EXHAUSTED", 2L, "HANDLER_FAILURE", 0L)).health();
        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("dlqPendingReplay", 2L);
    }

    @Test
    void unsupportedRepositoriesShouldRemainUpAndExposeUnknown() {
        Health health = indicator(1000, 1, 100, Map.of(), Map.of()).health();
        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("outboxPending", "unknown");
        assertThat(health.getDetails()).containsEntry("outboxStatsAvailable", false);
    }

    @Test
    void repositoryQueryFailureShouldBeDown() {
        OutboxProProperties properties = new OutboxProProperties();
        OutboxProHealthIndicator indicator = new OutboxProHealthIndicator(
                new FailedSnapshot(), properties.getMetrics().getHealth());
        assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
    }

    private OutboxProHealthIndicator indicator(long backlog, long dead, long dlq,
                                               Map<String, Long> outbox, Map<String, Long> dlqByReason) {
        OutboxProProperties properties = new OutboxProProperties();
        properties.getMetrics().getHealth().setOutboxBacklogThreshold(backlog);
        properties.getMetrics().getHealth().setOutboxDeadThreshold(dead);
        properties.getMetrics().getHealth().setDlqPendingThreshold(dlq);
        return new OutboxProHealthIndicator(
                new FixedSnapshot(outbox, dlqByReason), properties.getMetrics().getHealth());
    }

    private static final class FixedSnapshot extends OutboxProObservabilitySnapshot {
        private final Map<String, Long> outbox;
        private final Map<String, Long> dlqByReason;

        private FixedSnapshot(Map<String, Long> outbox, Map<String, Long> dlqByReason) {
            super(new EmptyRepository(), null, Duration.ofSeconds(30));
            this.outbox = outbox;
            this.dlqByReason = dlqByReason;
        }

        @Override public OutboxSnapshot get() {
            return new OutboxSnapshot(outbox, dlqByReason, false, false, java.time.Instant.now());
        }
    }

    private static final class FailedSnapshot extends OutboxProObservabilitySnapshot {
        private FailedSnapshot() {
            super(new EmptyRepository(), null, Duration.ofSeconds(30));
        }

        @Override public OutboxSnapshot get() {
            return new OutboxSnapshot(Map.of(), Map.of(), true, false, java.time.Instant.now());
        }
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
