package org.outboxpro.persistence.mysql;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Outbox 仓储的积压年龄查询：无积压返回 0，SQL 只统计待投递状态。 */
class JdbcOutboxRepositoryTest {

    @Test
    void oldestPendingAgeShouldBeReturnedFromAggregate() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), eq(Long.class), any(Object[].class))).thenReturn(42L);
        JdbcOutboxRepository repository = new JdbcOutboxRepository(jdbc);

        assertEquals(42L, repository.oldestPendingAgeSeconds(Instant.now()));
    }

    @Test
    void noPendingRowsShouldReportZeroInsteadOfNull() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), eq(Long.class), any(Object[].class))).thenReturn(null);
        JdbcOutboxRepository repository = new JdbcOutboxRepository(jdbc);

        assertEquals(0L, repository.oldestPendingAgeSeconds(Instant.now()));
    }

    @Test
    void negativeClockSkewShouldBeClampedToZero() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), eq(Long.class), any(Object[].class))).thenReturn(-5L);
        JdbcOutboxRepository repository = new JdbcOutboxRepository(jdbc);

        assertEquals(0L, repository.oldestPendingAgeSeconds(Instant.now()));
    }

    @Test
    void ageQueryShouldOnlyCoverDeliverableStatuses() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), eq(Long.class), any(Object[].class))).thenReturn(1L);
        JdbcOutboxRepository repository = new JdbcOutboxRepository(jdbc);

        repository.oldestPendingAgeSeconds(Instant.now());

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbc).queryForObject(sql.capture(), eq(Long.class), any(Object[].class));
        assertTrue(sql.getValue().contains("'PENDING'"));
        assertTrue(sql.getValue().contains("'RETRY_WAITING'"));
        assertTrue(sql.getValue().contains("next_retry_time"),
                "未到期的重试不应计入积压年龄");
    }
}
