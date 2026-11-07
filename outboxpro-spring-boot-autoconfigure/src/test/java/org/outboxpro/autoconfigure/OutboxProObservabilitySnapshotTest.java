package org.outboxpro.autoconfigure;

import org.junit.jupiter.api.Test;
import org.outboxpro.spi.deadletter.DeadLetterRepository;
import org.outboxpro.spi.persistence.OutboxRepository;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** 共享 TTL 快照：节流数据库查询、并发读不放大查询、查询异常不向调用方传播。 */
class OutboxProObservabilitySnapshotTest {

    @Test
    void repositoryFailureShouldDegradeToEmptySnapshotWithoutThrowing() {
        OutboxRepository repository = new OutboxRepository() {
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
            @Override public Map<String, Long> countByStatus() {
                throw new IllegalStateException("database unavailable");
            }
        };
        OutboxProObservabilitySnapshot snapshot =
                new OutboxProObservabilitySnapshot(repository, null, Duration.ofMillis(50));

        var value = snapshot.get();
        assertThat(value.outboxByStatus()).isEmpty();
        assertThat(value.pendingReplayCount()).isNull();
        assertThat(value.oldestPendingAgeSeconds())
                .as("查询失败时消息年龄应为未知，而不是伪造 0")
                .isNull();
    }

    @Test
    void oldestPendingAgeShouldComeFromRepositorySnapshot() {
        OutboxProObservabilitySnapshot snapshot = new OutboxProObservabilitySnapshot(
                new CountingRepository(new AtomicInteger(), new CountDownLatch(0), completedLatch()),
                null, Duration.ofSeconds(30));

        assertThat(snapshot.get().oldestPendingAgeSeconds()).isEqualTo(42L);
    }

    @Test
    void unsupportedDeadLetterRepositoryShouldStayUnknownInsteadOfZero() {
        OutboxRepository repository = new CountingRepository(new AtomicInteger(),
                new CountDownLatch(0), completedLatch());
        DeadLetterRepository unsupported = new DeadLetterRepository() {
            @Override public boolean beginDispatch(org.outboxpro.spi.deadletter.DeadLetterContext context) {
                return true;
            }
            @Override public void markPendingReplay(String eventId, String consumerName, java.time.Instant at) { }
            @Override public java.util.List<org.outboxpro.spi.deadletter.DeadLetterRecord> claimReplayByEventId(
                    String eventId, String owner, int maxReplayCount, String operator, String reason,
                    java.time.Instant at) { return java.util.List.of(); }
            @Override public void markReplaySucceeded(long id, String owner, java.time.Instant at) { }
            @Override public void releaseReplay(long id, String owner, String error, java.time.Instant at) { }
            @Override public long pendingReplayCount() { return 0; }
        };
        OutboxProObservabilitySnapshot snapshot = new OutboxProObservabilitySnapshot(
                repository, unsupported, Duration.ofSeconds(30));

        assertThat(snapshot.get().pendingReplayCount()).isNull();
    }

    @Test
    void expiredSnapshotShouldRefreshOnlyOnceAndCacheForTtl() {
        AtomicInteger queries = new AtomicInteger();
        OutboxRepository repository = new CountingRepository(queries,
                new CountDownLatch(0), completedLatch());
        OutboxProObservabilitySnapshot snapshot = new OutboxProObservabilitySnapshot(
                repository, null, Duration.ofSeconds(30));

        snapshot.get();
        snapshot.get();

        assertThat(queries.get()).isEqualTo(1);
    }

    private static CountDownLatch completedLatch() {
        CountDownLatch latch = new CountDownLatch(1);
        latch.countDown();
        return latch;
    }

    private static final class CountingRepository implements OutboxRepository {
        private final AtomicInteger queries;
        private final CountDownLatch ready;
        private final CountDownLatch start;

        private CountingRepository(AtomicInteger queries, CountDownLatch ready, CountDownLatch start) {
            this.queries = queries;
            this.ready = ready;
            this.start = start;
        }

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
        @Override public Long oldestPendingAgeSeconds(java.time.Instant now) {
            return 42L;
        }
        @Override public Map<String, Long> countByStatus() {
            queries.incrementAndGet();
            if (ready.getCount() > 0) {
                ready.countDown();
                try {
                    assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                }
            }
            return Map.of("PENDING", 1L, "PROCESSING", 0L, "RETRY_WAITING", 0L, "SENT", 0L, "DEAD", 0L);
        }
    }
}
