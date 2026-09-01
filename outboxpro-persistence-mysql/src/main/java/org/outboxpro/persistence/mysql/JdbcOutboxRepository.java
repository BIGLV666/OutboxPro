package org.outboxpro.persistence.mysql;

import org.outboxpro.spi.persistence.OutboxRecord;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 基于 Spring JDBC 的 MySQL Outbox Repository，支持多实例安全认领。
 * 依赖 MySQL 8 的 {@code FOR UPDATE SKIP LOCKED} 避免 Relay 实例互相阻塞。
 *
 * <p>类不能声明为 final：{@code claimBatch} 等方法上的 {@code @Transactional}
 * 需要 Spring 生成 CGLIB 子类代理，final 类会导致应用启动失败。</p>
 */
public class JdbcOutboxRepository implements OutboxRepository {
    private final JdbcTemplate jdbc;

    /** @param jdbc Spring JDBC 模板。 */
    public JdbcOutboxRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 在调用方当前事务中插入 PENDING Outbox 记录。
     * 此方法不自行开启事务，确保能够和业务表写入共同回滚。
     *
     * @param record 待写入的 Outbox 记录
     */
    @Override
    public void insert(OutboxRecord record) {
        String sql = """
                INSERT INTO outboxpro_outbox (
                    event_id, event_type, schema_version, producer,
                    exchange_name, routing_key, payload_json,
                    trace_id, correlation_id, causation_id,
                    status, attempt_count, next_retry_time, updated_time, version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NOW(), 0)
                """;
        jdbc.update(sql,
                record.eventId(), record.eventType(), record.schemaVersion(), record.producer(),
                record.exchangeName(), record.routingKey(), record.payloadJson(),
                record.traceId(), record.correlationId(), record.causationId(), record.status());
    }

    /**
     * 在短事务中批量认领可投递的 Outbox 记录。
     *
     * <p>先使用行锁选出候选记录，再通过一次批量 UPDATE 完成状态迁移，避免原先按记录循环执行 SQL。
     * 候选记录已经被当前事务锁定，因此无需为每一行再次发起数据库往返。</p>
     *
     * @param owner 当前 Relay 实例标识
     * @param batchSize 最大认领数量
     * @param now 当前时间
     * @param claimUntil 认领租约到期时间
     * @return 成功被当前实例认领的记录
     */
    @Override
    @Transactional
    public List<OutboxRecord> claimBatch(String owner, int batchSize, Instant now, Instant claimUntil) {
        if (batchSize <= 0) {
            return List.of();
        }
        String selectSql = """
                SELECT id, event_id, event_type, schema_version, producer,
                       exchange_name, routing_key, payload_json,
                       trace_id, correlation_id, causation_id,
                       status, attempt_count, next_retry_time, claim_owner, claimed_time
                FROM outboxpro_outbox
                WHERE status = 'PENDING'
                   OR (status = 'RETRY_WAITING'
                       AND (next_retry_time IS NULL OR next_retry_time <= ?))
                ORDER BY id
                LIMIT ?
                FOR UPDATE SKIP LOCKED
                """;
        List<OutboxRecord> candidates = jdbc.query(selectSql, this::map, Timestamp.from(now), batchSize);
        if (candidates.isEmpty()) {
            return List.of();
        }

        // 候选行已在当前事务中加锁，因此把所有 ID 合并成一次条件更新，消除 N 次 UPDATE。
        String placeholders = candidates.stream().map(ignored -> "?").collect(Collectors.joining(","));
        String updateSql = """
                UPDATE outboxpro_outbox
                SET status = 'PROCESSING',
                    claim_owner = ?,
                    claimed_time = ?,
                    updated_time = NOW(),
                    version = version + 1
                WHERE id IN (%s)
                  AND status IN ('PENDING', 'RETRY_WAITING')
                """.formatted(placeholders);
        List<Object> arguments = new ArrayList<>(2 + candidates.size());
        arguments.add(owner);
        arguments.add(Timestamp.from(claimUntil));
        candidates.forEach(candidate -> arguments.add(candidate.id()));
        jdbc.update(updateSql, arguments.toArray());

        // 由于候选行被行锁保护，条件更新不会被其他 Relay 抢走；统一构造 PROCESSING 返回值。
        return candidates.stream()
                .map(candidate -> toProcessing(candidate, owner, claimUntil))
                .toList();
    }

    /**
     * 将取得 RabbitMQ Publisher Confirm 的消息标记为 SENT。
     * owner 条件确保只有真正认领该记录的 Relay 能完成状态迁移。
     *
     * @param id Outbox 主键
     * @param owner 当前 Relay 实例标识
     * @param sentAt 发布确认时间
     */
    @Override
    public void markSent(long id, String owner, Instant sentAt) {
        jdbc.update("""
                UPDATE outboxpro_outbox
                SET status = 'SENT', sent_time = ?, updated_time = NOW(),
                    claim_owner = NULL, claimed_time = NULL, version = version + 1
                WHERE id = ? AND status = 'PROCESSING' AND claim_owner = ?
                """, Timestamp.from(sentAt), id, owner);
    }

    /**
     * 将可重试的投递失败记录释放租约，并安排下次 Relay 重试。
     *
     * @param id Outbox 主键
     * @param owner 当前 Relay 实例标识
     * @param attempt 当前尝试次数
     * @param nextRetryAt 下次重试时间
     * @param errorType 错误类型
     * @param errorMessage 脱敏后的错误信息
     */
    @Override
    public void markRetryWaiting(long id, String owner, int attempt, Instant nextRetryAt,
                                 String errorType, String errorMessage) {
        jdbc.update("""
                UPDATE outboxpro_outbox
                SET status = 'RETRY_WAITING', attempt_count = ?, next_retry_time = ?,
                    last_error_type = ?, last_error_message = ?, updated_time = NOW(),
                    claim_owner = NULL, claimed_time = NULL, version = version + 1
                WHERE id = ? AND status = 'PROCESSING' AND claim_owner = ?
                """, attempt, Timestamp.from(nextRetryAt), errorType, truncate(errorMessage), id, owner);
    }

    /**
     * 将超过重试上限的投递失败记录标记为 DEAD，并释放租约。
     *
     * @param id Outbox 主键
     * @param owner 当前 Relay 实例标识
     * @param attempt 最终尝试次数
     * @param errorType 错误类型
     * @param errorMessage 脱敏后的错误信息
     */
    @Override
    public void markDead(long id, String owner, int attempt, String errorType, String errorMessage) {
        jdbc.update("""
                UPDATE outboxpro_outbox
                SET status = 'DEAD', attempt_count = ?, last_error_type = ?, last_error_message = ?,
                    updated_time = NOW(), claim_owner = NULL, claimed_time = NULL, version = version + 1
                WHERE id = ? AND status = 'PROCESSING' AND claim_owner = ?
                """, attempt, errorType, truncate(errorMessage), id, owner);
    }

    /**
     * 恢复租约超时的 PROCESSING 记录。
     *
     * @param now 当前时间
     * @return 被恢复为 PENDING 的记录数量
     */
    @Override
    public int recoverExpiredClaims(Instant now) {
        return jdbc.update("""
                UPDATE outboxpro_outbox
                SET status = 'PENDING', claim_owner = NULL, claimed_time = NULL,
                    updated_time = NOW(), version = version + 1
                WHERE status = 'PROCESSING' AND claimed_time < ?
                """, Timestamp.from(now));
    }

    /** 将候选记录转换为带当前租约信息的 PROCESSING 记录。 */
    private OutboxRecord toProcessing(OutboxRecord candidate, String owner, Instant claimUntil) {
        return new OutboxRecord(candidate.id(), candidate.eventId(), candidate.eventType(),
                candidate.schemaVersion(), candidate.producer(), candidate.exchangeName(),
                candidate.routingKey(), candidate.payloadJson(), candidate.traceId(),
                candidate.correlationId(), candidate.causationId(), "PROCESSING",
                candidate.attemptCount(), candidate.nextRetryTime(), owner, claimUntil);
    }

    /** 限制错误文本长度，避免异常信息撑大数据库行和日志。 */
    private String truncate(String value) {
        return value == null ? null : value.substring(0, Math.min(value.length(), 2000));
    }

    /** 将 JDBC 查询结果映射为 Outbox 记录。 */
    private OutboxRecord map(ResultSet resultSet, int rowNumber) throws SQLException {
        Timestamp nextRetryTime = resultSet.getTimestamp("next_retry_time");
        Timestamp claimedTime = resultSet.getTimestamp("claimed_time");
        return new OutboxRecord(
                resultSet.getLong("id"), resultSet.getString("event_id"), resultSet.getString("event_type"),
                resultSet.getString("schema_version"), resultSet.getString("producer"),
                resultSet.getString("exchange_name"), resultSet.getString("routing_key"),
                resultSet.getString("payload_json"), resultSet.getString("trace_id"),
                resultSet.getString("correlation_id"), resultSet.getString("causation_id"),
                resultSet.getString("status"), resultSet.getInt("attempt_count"),
                nextRetryTime == null ? null : nextRetryTime.toInstant(),
                resultSet.getString("claim_owner"), claimedTime == null ? null : claimedTime.toInstant());
    }

    /**
     * 按条件分页检索 Outbox 消息。
     * 状态字段先过白名单再进 SQL，事件类型走参数绑定，杜绝检索输入带来的注入风险。
     *
     * @param query 检索条件
     * @return 按 id 倒序排列的命中记录
     */
    @Override
    public List<OutboxRecord> findMessages(org.outboxpro.spi.persistence.OutboxQuery query) {
        String status = sanitizeStatus(query.status());
        if (status == null && query.status() != null && !query.status().isBlank()) {
            return List.of();
        }
        StringBuilder where = new StringBuilder();
        List<Object> args = new ArrayList<>();
        if (status != null) {
            where.append("status = ?");
            args.add(status);
        }
        if (query.eventType() != null && !query.eventType().isBlank()) {
            appendAnd(where);
            where.append("event_type = ?");
            args.add(query.eventType());
        }
        String sql = """
                SELECT id, event_id, event_type, schema_version, producer,
                       exchange_name, routing_key, payload_json,
                       trace_id, correlation_id, causation_id,
                       status, attempt_count, next_retry_time, claim_owner, claimed_time
                FROM outboxpro_outbox
                %s
                ORDER BY id DESC
                LIMIT ? OFFSET ?
                """.formatted(where.isEmpty() ? "" : "WHERE " + where);
        args.add(Math.min(query.limit(), MAX_QUERY_LIMIT));
        args.add(Math.max(query.offset(), 0));
        return jdbc.query(sql, this::map, args.toArray());
    }

    /**
     * 统计检索条件命中的消息总数。
     *
     * @param query 检索条件
     * @return 命中总数
     */
    @Override
    public long countMessages(org.outboxpro.spi.persistence.OutboxQuery query) {
        String status = sanitizeStatus(query.status());
        if (status == null && query.status() != null && !query.status().isBlank()) {
            return 0L;
        }
        StringBuilder where = new StringBuilder();
        List<Object> args = new ArrayList<>();
        if (status != null) {
            where.append("status = ?");
            args.add(status);
        }
        if (query.eventType() != null && !query.eventType().isBlank()) {
            appendAnd(where);
            where.append("event_type = ?");
            args.add(query.eventType());
        }
        String sql = "SELECT COUNT(*) FROM outboxpro_outbox %s"
                .formatted(where.isEmpty() ? "" : "WHERE " + where);
        Long count = jdbc.queryForObject(sql, Long.class, args.toArray());
        return count == null ? 0L : count;
    }

    /**
     * 按事件 ID 精确查询单条 Outbox 消息（含载荷）。
     *
     * @param eventId 事件唯一 ID
     * @return 命中记录；不存在时为 {@code null}
     */
    @Override
    public OutboxRecord findByEventId(String eventId) {
        List<OutboxRecord> records = jdbc.query("""
                SELECT id, event_id, event_type, schema_version, producer,
                       exchange_name, routing_key, payload_json,
                       trace_id, correlation_id, causation_id,
                       status, attempt_count, next_retry_time, claim_owner, claimed_time
                FROM outboxpro_outbox
                WHERE event_id = ?
                """, this::map, eventId);
        return records.isEmpty() ? null : records.get(0);
    }

    /**
     * 把 DEAD 消息复位为 PENDING，交还 Relay 重新投递。
     * 条件更新只允许 DEAD 状态参与迁移，避免与正常状态机互相破坏。
     *
     * @param eventId 事件唯一 ID
     * @return 是否成功复位
     */
    @Override
    public boolean resetDeadForReplay(String eventId) {
        int updated = jdbc.update("""
                UPDATE outboxpro_outbox
                SET status = 'PENDING', attempt_count = 0, next_retry_time = NULL,
                    claim_owner = NULL, claimed_time = NULL,
                    updated_time = NOW(), version = version + 1
                WHERE event_id = ? AND status = 'DEAD'
                """, eventId);
        return updated == 1;
    }

    /** 运维检索单页行数上限，防止一次性拉取大结果集。 */
    private static final int MAX_QUERY_LIMIT = 200;

    /** 校验状态过滤值：命中白名单返回大写形式，空白视为不过滤，其余返回 null 表示无结果。 */
    private String sanitizeStatus(String status) {
        if (status == null || status.isBlank()) {
            return null;
        }
        String normalized = status.trim().toUpperCase();
        return org.outboxpro.spi.persistence.OutboxQuery.ALLOWED_STATUSES.contains(normalized) ? normalized : null;
    }

    /** 向动态 WHERE 子句追加 AND 连接符。 */
    private void appendAnd(StringBuilder where) {
        if (!where.isEmpty()) {
            where.append(" AND ");
        }
    }
}
