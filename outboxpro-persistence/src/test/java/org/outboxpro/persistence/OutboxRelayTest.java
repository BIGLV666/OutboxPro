package org.outboxpro.persistence;

import org.junit.jupiter.api.Test;
import org.outboxpro.core.metrics.OutboxMetrics;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.spi.persistence.OutboxRecord;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.outboxpro.spi.transport.MessagePublisher;
import org.outboxpro.spi.transport.MessagePublisher.PublishOutcome;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OutboxRelayTest {
    @Test
    void successfulPublishMarksSent() {
        FakeRepository repository = new FakeRepository();
        OutboxRelay relay = new OutboxRelay(repository, record -> {}, RetryPolicy.defaults(), 10, Duration.ofSeconds(30));
        relay.relayOnce();
        assertTrue(repository.sent);
        assertFalse(repository.dead);
    }

    @Test
    void exhaustedPublishMarksDead() {
        FakeRepository repository = new FakeRepository();
        MessagePublisher publisher = record -> { throw new IllegalStateException("broker unavailable"); };
        RetryPolicy oneAttempt = new RetryPolicy(true, 1, Duration.ZERO, 2, Duration.ZERO);
        new OutboxRelay(repository, publisher, oneAttempt, 10, Duration.ofSeconds(30)).relayOnce();
        assertTrue(repository.dead);
        assertFalse(repository.sent);
    }

    /** 批量发布：混合结果必须按序对应到各记录的状态迁移。 */
    @Test
    void batchOutcomesDriveStateTransitionsInOrder() {
        BatchRepository repository = new BatchRepository(2);
        OutboxMetrics metrics = new RecordingMetrics();
        MessagePublisher publisher = new MessagePublisher() {
            @Override public void publish(OutboxRecord record) {
                throw new UnsupportedOperationException("should not be called when publishAll is overridden");
            }
            @Override public java.util.List<PublishOutcome> publishAll(java.util.List<OutboxRecord> records) {
                return java.util.List.of(PublishOutcome.ok(), PublishOutcome.failed(new IllegalStateException("nack")));
            }
        };
        OutboxRelay relay = new OutboxRelay(repository, publisher, new RetryPolicy(true, 3, Duration.ZERO, 2, Duration.ZERO),
                10, Duration.ofSeconds(30), metrics);
        relay.relayOnce();
        assertTrue(repository.sentIds.contains(1L), "成功结果应迁移 SENT");
        assertTrue(repository.retryWaitingIds.contains(2L), "失败结果应迁移 RETRY_WAITING");
        assertFalse(repository.deadIds.contains(2L));
    }

    /** 批量结果数量与记录数不一致属于实现契约破坏，必须抛出而不是错位迁移。 */
    @Test
    void mismatchedOutcomeSizeIsRejected() {
        BatchRepository repository = new BatchRepository(2);
        MessagePublisher publisher = new MessagePublisher() {
            @Override public void publish(OutboxRecord record) { }
            @Override public java.util.List<PublishOutcome> publishAll(java.util.List<OutboxRecord> records) {
                return java.util.List.of(PublishOutcome.ok());
            }
        };
        OutboxRelay relay = new OutboxRelay(repository, publisher, RetryPolicy.defaults(), 10, Duration.ofSeconds(30));
        assertThrows(IllegalStateException.class, relay::relayOnce);
    }

    /** 租约丢失（状态迁移 0 行生效）必须通过 metrics 暴露。 */
    @Test
    void leaseLostIsReportedThroughMetrics() {
        BatchRepository repository = new BatchRepository(1) {
            @Override public boolean transitionToSent(long id, String owner, Instant sentAt) { return false; }
        };
        RecordingMetrics metrics = new RecordingMetrics();
        MessagePublisher publisher = new MessagePublisher() {
            @Override public void publish(OutboxRecord record) { }
        };
        new OutboxRelay(repository, publisher, RetryPolicy.defaults(), 10, Duration.ofSeconds(30), metrics).relayOnce();
        assertEquals(1, metrics.leaseLost);
    }

    /** 配置了日志 Sink 时，生产端发布阶段会写入 PUBLISH 生命周期记录。 */
    @Test
    void publishStageIsLoggedWhenSinkConfigured() {
        FakeRepository repository = new FakeRepository();
        RecordingSink sink = new RecordingSink();
        OutboxRelay relay = new OutboxRelay(repository, record -> { }, RetryPolicy.defaults(),
                10, Duration.ofSeconds(30), OutboxMetrics.NOOP, sink);
        relay.relayOnce();
        assertEquals(1, sink.records.size());
        assertEquals(org.outboxpro.spi.observability.MessageStage.PUBLISH, sink.records.get(0).stage());
        assertEquals(org.outboxpro.spi.observability.MessageStatus.SUCCESS, sink.records.get(0).status());
        assertEquals("event-1", sink.records.get(0).eventId());
    }

    private static final class FakeRepository implements OutboxRepository {
        private final OutboxRecord record = new OutboxRecord(1, "event-1", "demo.created", "v1", "demo", "demo.exchange", "demo.created", "{}", null, null, null, "PENDING", 0, null, null, null);
        boolean sent; boolean dead;
        @Override public void insert(OutboxRecord record) { }
        @Override public List<OutboxRecord> claimBatch(String owner, int batchSize, Instant now, Instant claimUntil) { return List.of(record); }
        @Override public void markSent(long id, String owner, Instant sentAt) { sent = true; }
        @Override public void markRetryWaiting(long id, String owner, int attempt, Instant nextRetryAt, String errorType, String errorMessage) { }
        @Override public void markDead(long id, String owner, int attempt, String errorType, String errorMessage) { dead = true; }
        @Override public int recoverExpiredClaims(Instant now) { return 0; }
    }

    /** 返回多条记录的仓储桩：记录各状态迁移命中的主键。 */
    private static class BatchRepository implements OutboxRepository {
        private final List<OutboxRecord> records;
        final List<Long> sentIds = new java.util.ArrayList<>();
        final List<Long> retryWaitingIds = new java.util.ArrayList<>();
        final List<Long> deadIds = new java.util.ArrayList<>();

        BatchRepository(int count) {
            java.util.List<OutboxRecord> list = new java.util.ArrayList<>();
            for (int i = 1; i <= count; i++) {
                list.add(new OutboxRecord(i, "event-" + i, "demo.created", "v1", "demo",
                        "demo.exchange", "demo.created", "{}", null, null, null, "PENDING", 0, null, null, null));
            }
            this.records = List.copyOf(list);
        }

        @Override public void insert(OutboxRecord record) { }
        @Override public List<OutboxRecord> claimBatch(String owner, int batchSize, Instant now, Instant claimUntil) { return records; }
        @Override public boolean transitionToSent(long id, String owner, Instant sentAt) { sentIds.add(id); return true; }
        @Override public boolean transitionToRetryWaiting(long id, String owner, int attempt, Instant nextRetryAt,
                                                          String errorType, String errorMessage) { retryWaitingIds.add(id); return true; }
        @Override public boolean transitionToDead(long id, String owner, int attempt, String errorType, String errorMessage) { deadIds.add(id); return true; }
        @Override public void markSent(long id, String owner, Instant sentAt) { sentIds.add(id); }
        @Override public void markRetryWaiting(long id, String owner, int attempt, Instant nextRetryAt, String errorType, String errorMessage) { retryWaitingIds.add(id); }
        @Override public void markDead(long id, String owner, int attempt, String errorType, String errorMessage) { deadIds.add(id); }
        @Override public int recoverExpiredClaims(Instant now) { return 0; }
    }

    /** 记录租约丢失上报次数的指标桩。 */
    private static final class RecordingMetrics implements OutboxMetrics {
        int leaseLost;
        @Override public void relayLeaseLost(String eventType, String producer) { leaseLost++; }
    }

    /** 捕获全部生命周期记录的日志 Sink 桩。 */
    private static final class RecordingSink implements org.outboxpro.spi.observability.MessageLogSink {
        final java.util.List<org.outboxpro.spi.observability.MessageTraceRecord> records = new java.util.ArrayList<>();
        @Override public void append(org.outboxpro.spi.observability.MessageTraceRecord record) { records.add(record); }
    }
}
