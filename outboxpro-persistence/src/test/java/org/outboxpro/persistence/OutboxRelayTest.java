package org.outboxpro.persistence;

import org.junit.jupiter.api.Test;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.spi.persistence.OutboxRecord;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.outboxpro.spi.transport.MessagePublisher;

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
}
