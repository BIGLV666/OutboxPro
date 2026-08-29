package org.outboxpro.persistence.mysql;

import org.junit.jupiter.api.Test;
import org.outboxpro.spi.deadletter.DeadLetterContext;
import org.outboxpro.spi.deadletter.DeadLetterReason;
import org.outboxpro.spi.deadletter.DeadLetterReasonCode;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * MySQL 死信台账租约状态测试。
 *
 * <p>通过 JDBC 调用结果模拟唯一键冲突和条件更新竞争，验证活跃分派不会被误认为已可靠处理。</p>
 */
class JdbcDeadLetterRepositoryTest {

    @Test
    void newRecord_shouldAcquireDispatchLeaseWithSingleInsert() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(1);
        JdbcDeadLetterRepository repository = new JdbcDeadLetterRepository(jdbc, 32);

        boolean acquired = repository.beginDispatch(context(), "owner-1", Instant.now().plusSeconds(60));

        assertTrue(acquired);
        verify(jdbc).update(anyString(), any(Object[].class));
    }

    @Test
    void expiredDispatch_shouldBeReclaimedByConditionalUpdate() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        // 第一次 INSERT 命中唯一键，第二次条件 UPDATE 成功接管过期租约。
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(0, 1);
        JdbcDeadLetterRepository repository = new JdbcDeadLetterRepository(jdbc, 32);

        assertTrue(repository.beginDispatch(context(), "owner-2", Instant.now().plusSeconds(60)));
    }

    @Test
    void pendingReplay_shouldReportReliableDuplicateWithoutRedispatch() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(0, 0);
        when(jdbc.queryForObject(anyString(), eq(String.class), any(Object[].class)))
                .thenReturn("PENDING_REPLAY");
        JdbcDeadLetterRepository repository = new JdbcDeadLetterRepository(jdbc, 32);

        assertFalse(repository.beginDispatch(context(), "owner-3", Instant.now().plusSeconds(60)));
    }

    @Test
    void activeDispatch_shouldThrowSoRabbitMessageIsRequeued() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(0, 0);
        when(jdbc.queryForObject(anyString(), eq(String.class), any(Object[].class)))
                .thenReturn("DISPATCHING");
        JdbcDeadLetterRepository repository = new JdbcDeadLetterRepository(jdbc, 32);

        assertThrows(IllegalStateException.class, () ->
                repository.beginDispatch(context(), "owner-4", Instant.now().plusSeconds(60)));
    }

    @Test
    void currentOwner_shouldMarkPendingAndUpdateCounterOnce() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        // 首次 UPDATE 完成状态迁移，第二次 UPDATE 写入固定分桶计数。
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(1, 1);
        JdbcDeadLetterRepository repository = new JdbcDeadLetterRepository(jdbc, 32);

        assertTrue(repository.markPendingReplay("evt-1", "consumer-1", "owner-5", Instant.now()));
    }

    /** 创建满足持久化约束的死信上下文。 */
    private DeadLetterContext context() {
        return new DeadLetterContext(
                "evt-1", "OrderCreated", "consumer-1", "order.queue",
                "order.exchange", "order.created", "{}", 5,
                new DeadLetterReason(DeadLetterReasonCode.RETRY_EXHAUSTED, true, true,
                        RuntimeException.class.getName(), "failed"),
                Instant.now());
    }
}
