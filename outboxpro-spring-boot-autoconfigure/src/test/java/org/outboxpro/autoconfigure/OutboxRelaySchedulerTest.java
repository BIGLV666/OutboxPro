package org.outboxpro.autoconfigure;

import org.junit.jupiter.api.Test;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.persistence.OutboxRelay;
import org.outboxpro.spi.persistence.OutboxRecord;
import org.outboxpro.spi.persistence.OutboxRepository;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Relay 调度器：多工作线程并行、空闲指数退避、单轮异常不杀死调度线程。 */
class OutboxRelaySchedulerTest {

    @Test
    void idleBackoffShouldDoubleUntilCapAndNeverGoBelowBase() {
        Duration base = Duration.ofSeconds(1);
        Duration max = Duration.ofSeconds(5);

        assertThat(OutboxRelayScheduler.nextDelay(base, max, 0)).isEqualTo(base);
        assertThat(OutboxRelayScheduler.nextDelay(base, max, 1)).isEqualTo(Duration.ofSeconds(2));
        assertThat(OutboxRelayScheduler.nextDelay(base, max, 2)).isEqualTo(Duration.ofSeconds(4));
        assertThat(OutboxRelayScheduler.nextDelay(base, max, 3)).isEqualTo(max);
        assertThat(OutboxRelayScheduler.nextDelay(base, max, 30)).isEqualTo(max);
        assertThat(OutboxRelayScheduler.nextDelay(base, Duration.ofMillis(100), 2))
                .as("上限小于基础间隔时仍按基础间隔执行，避免退化成热轮询")
                .isEqualTo(base);
    }

    @Test
    void multipleWorkersShouldClaimConcurrently() throws Exception {
        ConcurrentClaimRepository repository = new ConcurrentClaimRepository();
        OutboxRelay relay = new OutboxRelay(repository, record -> { }, RetryPolicy.defaults(),
                10, Duration.ofSeconds(30));
        OutboxRelayScheduler scheduler = new OutboxRelayScheduler(
                relay, 2, Duration.ofMillis(20), Duration.ofMillis(100));

        try {
            scheduler.start();
            assertThat(repository.bothWorkersClaimed.await(5, TimeUnit.SECONDS))
                    .as("两个工作线程应能同时进入 claimBatch")
                    .isTrue();
            assertThat(repository.maxConcurrentClaims).isGreaterThanOrEqualTo(2);
        } finally {
            scheduler.stop();
        }
    }

    @Test
    void relayExceptionShouldNotKillSchedulingLoop() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch recovered = new CountDownLatch(1);
        OutboxRepository repository = new RepositoryStub() {
            @Override
            public List<OutboxRecord> claimBatch(String owner, int batchSize, Instant now, Instant claimUntil) {
                if (calls.incrementAndGet() <= 2) {
                    throw new IllegalStateException("simulated database outage");
                }
                recovered.countDown();
                return List.of();
            }
        };
        OutboxRelay relay = new OutboxRelay(repository, record -> { }, RetryPolicy.defaults(),
                10, Duration.ofSeconds(30));
        OutboxRelayScheduler scheduler = new OutboxRelayScheduler(
                relay, 1, Duration.ofMillis(10), Duration.ofMillis(10));

        try {
            scheduler.start();
            assertThat(recovered.await(5, TimeUnit.SECONDS))
                    .as("异常后调度器应继续下一轮，而不是永久停止")
                    .isTrue();
        } finally {
            scheduler.stop();
        }
    }

    /** 前两次 claim 用栅栏强制重叠，证明 workers 是并行执行而不是串行轮询。 */
    private static final class ConcurrentClaimRepository extends RepositoryStub {
        private final CyclicBarrier barrier = new CyclicBarrier(2);
        private final CountDownLatch bothWorkersClaimed = new CountDownLatch(2);
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicInteger active = new AtomicInteger();
        private volatile int maxConcurrentClaims;

        @Override
        public List<OutboxRecord> claimBatch(String owner, int batchSize, Instant now, Instant claimUntil) {
            int current = active.incrementAndGet();
            synchronized (this) {
                maxConcurrentClaims = Math.max(maxConcurrentClaims, current);
            }
            try {
                if (calls.incrementAndGet() <= 2) {
                    barrier.await(2, TimeUnit.SECONDS);
                }
                bothWorkersClaimed.countDown();
                return List.of(record(1));
            } catch (Exception interrupted) {
                return List.of();
            } finally {
                active.decrementAndGet();
            }
        }
    }

    /** 仅实现必要状态迁移的仓储桩。 */
    private static class RepositoryStub implements OutboxRepository {
        @Override public void insert(OutboxRecord record) { }
        @Override public List<OutboxRecord> claimBatch(String owner, int batchSize, Instant now, Instant claimUntil) {
            return List.of();
        }
        @Override public void markSent(long id, String owner, Instant sentAt) { }
        @Override public void markRetryWaiting(long id, String owner, int attempt, Instant nextRetryAt,
                                               String errorType, String errorMessage) { }
        @Override public void markDead(long id, String owner, int attempt, String errorType, String errorMessage) { }
        @Override public int recoverExpiredClaims(Instant now) { return 0; }
        @Override public boolean transitionToSent(long id, String owner, Instant sentAt) { return true; }
    }

    private static OutboxRecord record(long id) {
        return new OutboxRecord(id, "event-" + id, "it.event", "v1", "test", "it.exchange", "it.event",
                "{}", null, null, null, "PENDING", 0, null, null, null);
    }
}
