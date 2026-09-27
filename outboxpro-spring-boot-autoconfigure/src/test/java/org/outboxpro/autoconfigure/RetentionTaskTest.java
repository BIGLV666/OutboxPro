package org.outboxpro.autoconfigure;

import org.junit.jupiter.api.Test;
import org.outboxpro.spi.deadletter.DeadLetterContext;
import org.outboxpro.spi.deadletter.DeadLetterRecord;
import org.outboxpro.spi.deadletter.DeadLetterRepository;
import org.outboxpro.spi.persistence.InboxRecord;
import org.outboxpro.spi.persistence.InboxRepository;
import org.outboxpro.spi.persistence.OutboxRecord;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link RetentionTask} 单元测试：分批删除循环按批推进并正确终止，
 * 运行互斥守卫阻止重复执行，保留策略关闭时返回跳过结果。
 */
class RetentionTaskTest {

    /** 记录各批删除调用的 Outbox 仓储桩：按预设脚本依次返回删除行数。 */
    private static final class FakeOutboxRepository implements OutboxRepository {
        final List<Integer> batchRequests = new ArrayList<>();
        private final int[] scriptedDeletes;
        private int call;

        FakeOutboxRepository(int... scriptedDeletes) { this.scriptedDeletes = scriptedDeletes; }

        @Override
        public int purgeSentBefore(Instant cutoff, int limit) {
            batchRequests.add(limit);
            return scriptedDeletes[Math.min(call++, scriptedDeletes.length - 1)];
        }

        @Override public void insert(OutboxRecord record) { }
        @Override public List<OutboxRecord> claimBatch(String owner, int batchSize, Instant now, Instant claimUntil) { return List.of(); }
        @Override public void markSent(long id, String owner, Instant sentAt) { }
        @Override public void markRetryWaiting(long id, String owner, int attempt, Instant nextRetryAt, String errorType, String errorMessage) { }
        @Override public void markDead(long id, String owner, int attempt, String errorType, String errorMessage) { }
        @Override public int recoverExpiredClaims(Instant now) { return 0; }
    }

    /** 空的 Inbox 仓储桩。 */
    private static final class NoopInboxRepository implements InboxRepository {
        @Override public boolean tryStart(String consumerName, InboxRecord record) { return false; }
        @Override public void markSuccess(String consumerName, String eventId) { }
        @Override public void markFailed(String consumerName, String eventId, String errorMessage) { }
        @Override public void markIgnored(String consumerName, String eventId, String errorMessage) { }
        @Override public int purgeSuccessBefore(Instant cutoff, int limit) { return 0; }
    }

    /** 不做任何删除的死信仓储桩（台账未启用时传 null 亦可）。 */
    private static final class NoopDeadLetterRepository implements DeadLetterRepository {
        @Override public boolean beginDispatch(DeadLetterContext context) { return true; }
        @Override public void markPendingReplay(String eventId, String consumerName, Instant at) { }
        @Override public List<DeadLetterRecord> claimReplayByEventId(String eventId, String owner, int maxReplayCount, String operator, String reason, Instant at) { return List.of(); }
        @Override public void markReplaySucceeded(long id, String owner, Instant at) { }
        @Override public void releaseReplay(long id, String owner, String error, Instant at) { }
        @Override public long pendingReplayCount() { return 0; }
    }

    /** 只记录 update 调用次数的 JdbcTemplate 桩。 */
    private static final class RecordingJdbcTemplate extends JdbcTemplate {
        int updateCalls;
        @Override public int update(String sql, Object... args) { updateCalls++; return 0; }
    }

    private static OutboxProProperties.Retention config(int batchSize, int maxRowsPerCycle, boolean enabled) {
        OutboxProProperties.Retention config = new OutboxProProperties.Retention();
        config.setBatchSize(batchSize);
        config.setMaxRowsPerCycle(maxRowsPerCycle);
        config.setEnabled(enabled);
        return config;
    }

    private static RetentionTask task(OutboxRepository outbox, OutboxProProperties.Retention config) {
        return new RetentionTask(outbox, new NoopInboxRepository(), new NoopDeadLetterRepository(),
                new RecordingJdbcTemplate(), config);
    }

    /** 删除量恰好等于批大小时继续推进，直到某批不足批大小才停止，且不越过单轮上限。 */
    @Test
    void purgeLoopAdvancesBatchByBatchAndStopsOnShortBatch() {
        FakeOutboxRepository outbox = new FakeOutboxRepository(2, 2, 1);
        RetentionTask.PurgeResult result = task(outbox, config(2, 10, true)).purge();
        assertThat(result.skipped()).isFalse();
        assertThat(result.outboxSent()).isEqualTo(5);
        // 每批 LIMIT 恒等于批大小；第三批实际删除 1 < 2 触发终止。
        assertThat(outbox.batchRequests).containsExactly(2, 2, 2);
    }

    /** 单轮上限会截断推进：两批满批后达到 maxRowsPerCycle=4，不再发起第三批。 */
    @Test
    void purgeLoopRespectsMaxRowsPerCycle() {
        FakeOutboxRepository outbox = new FakeOutboxRepository(2, 2, 2);
        RetentionTask.PurgeResult result = task(outbox, config(2, 4, true)).purge();
        assertThat(result.outboxSent()).isEqualTo(4);
        assertThat(outbox.batchRequests).containsExactly(2, 2);
    }

    /** 保留策略关闭时返回 skipped，不发起任何删除。 */
    @Test
    void disabledRetentionSkipsAllPurges() {
        FakeOutboxRepository outbox = new FakeOutboxRepository(0);
        RetentionTask.PurgeResult result = task(outbox, config(2, 10, false)).purge();
        assertThat(result.skipped()).isTrue();
        assertThat(outbox.batchRequests).isEmpty();
    }

    /** 顺序两次调用互不干扰（互斥守卫在串行场景下必须正确释放）。 */
    @Test
    void sequentialPurgesBothExecute() {
        FakeOutboxRepository outbox = new FakeOutboxRepository(1, 1);
        RetentionTask retentionTask = task(outbox, config(2, 10, true));
        RetentionTask.PurgeResult first = retentionTask.purge();
        RetentionTask.PurgeResult second = retentionTask.purge();
        assertThat(first.skipped()).isFalse();
        assertThat(second.skipped()).isFalse();
        assertThat(outbox.batchRequests).hasSize(2);
    }
}
