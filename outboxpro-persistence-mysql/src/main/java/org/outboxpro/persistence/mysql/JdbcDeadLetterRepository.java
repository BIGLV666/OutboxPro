package org.outboxpro.persistence.mysql;

import org.outboxpro.spi.deadletter.DeadLetterContext;
import org.outboxpro.spi.deadletter.DeadLetterReason;
import org.outboxpro.spi.deadletter.DeadLetterReasonCode;
import org.outboxpro.spi.deadletter.DeadLetterRecord;
import org.outboxpro.spi.deadletter.DeadLetterRepository;
import org.outboxpro.spi.deadletter.DeadLetterStatus;
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
 * 基于 MySQL 的死信台账实现。
 *
 * <p>待重放数量通过固定数量的计数桶维护，监控读取计数桶而不是扫描不断增长的死信明细表。
 * 重放认领使用一次批量状态更新，避免按记录循环执行数据库更新。</p>
 *
 * <p>类不能声明为 final：方法上的 {@code @Transactional} 需要 Spring 生成 CGLIB 子类代理。</p>
 */
public class JdbcDeadLetterRepository implements DeadLetterRepository {
    private final JdbcTemplate jdbc;
    private final int counterBuckets;

    /**
     * 创建死信台账仓储。
     *
     * @param jdbc Spring JDBC 模板
     * @param counterBuckets 计数桶数量，必须为正数
     */
    public JdbcDeadLetterRepository(JdbcTemplate jdbc, int counterBuckets) {
        if (counterBuckets <= 0) {
            throw new IllegalArgumentException("counterBuckets must be positive");
        }
        this.jdbc = jdbc;
        this.counterBuckets = counterBuckets;
    }

    /**
     * 使用临时随机持有人创建或恢复一条死信分派记录。
     *
     * <p>该入口保留给旧调用方；新调用方应使用带 {@code owner} 和 {@code leaseUntil} 的重载，
     * 以便在死信策略失败时立即释放租约。</p>
     *
     * @param context 不可变死信上下文
     * @return 当前调用方是否需要执行死信策略
     */
    @Override
    @Transactional
    public boolean beginDispatch(DeadLetterContext context) {
        // 旧调用方没有租约标识，使用一次性 UUID 避免不同调用共享固定 owner。
        return beginDispatch(context, java.util.UUID.randomUUID().toString(), Instant.now().plusSeconds(60));
    }

    /**
     * 以数据库条件更新竞争死信分派租约。
     *
     * <p>正常的新消息只执行一次 {@code INSERT IGNORE}；只有唯一键冲突时才进入状态恢复路径。
     * 已可靠分派的记录返回 {@code false}，正在由其他实例分派的记录抛出异常，使上层 NACK，
     * 防止进程崩溃后的 RabbitMQ 重投递被误 ACK。</p>
     *
     * @param context 不可变死信上下文
     * @param owner 当前消费调用的唯一分派持有人
     * @param leaseUntil 分派租约到期时间
     * @return 当前调用是否取得分派权
     */
    @Override
    @Transactional
    public boolean beginDispatch(DeadLetterContext context, String owner, Instant leaseUntil) {
        DeadLetterReason reason = context.reason();
        int inserted = jdbc.update("""
                INSERT IGNORE INTO outboxpro_dead_letter (
                    event_id, event_type, consumer_name, queue_name,
                    original_exchange, original_routing_key, payload_json,
                    attempt_count, reason_code, reason_retryable, reason_retry_exhausted,
                    exception_type, exception_message, status, dispatch_owner, dispatch_until,
                    replay_count, version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'DISPATCHING', ?, ?, 0, 0)
                """,
                context.eventId(), context.eventType(), context.consumerName(), context.queue(),
                context.originalExchange(), context.originalRoutingKey(), context.payloadJson(), context.attempt(),
                reason.code().name(), reason.retryable(), reason.retryExhausted(),
                reason.exceptionType(), truncate(reason.exceptionMessage()), owner, Timestamp.from(leaseUntil));
        if (inserted == 1) {
            // 新记录通过唯一键保证只有一个消费者获得分派权。
            return true;
        }

        int claimed = jdbc.update("""
                UPDATE outboxpro_dead_letter
                SET status = 'DISPATCHING', event_type = ?, queue_name = ?,
                    original_exchange = ?, original_routing_key = ?, payload_json = ?,
                    attempt_count = ?, reason_code = ?, reason_retryable = ?,
                    reason_retry_exhausted = ?, exception_type = ?, exception_message = ?,
                    dispatch_owner = ?, dispatch_until = ?, replay_owner = NULL,
                    updated_time = NOW(6), version = version + 1
                WHERE event_id = ? AND consumer_name = ?
                  AND (
                      status = 'REPLAYED'
                      OR (status = 'DISPATCHING' AND (
                          dispatch_owner = ? OR dispatch_until IS NULL OR dispatch_until <= NOW(6)
                      ))
                  )
                """, context.eventType(), context.queue(), context.originalExchange(),
                context.originalRoutingKey(), context.payloadJson(), context.attempt(), reason.code().name(),
                reason.retryable(), reason.retryExhausted(), reason.exceptionType(),
                truncate(reason.exceptionMessage()), owner, Timestamp.from(leaseUntil), context.eventId(),
                context.consumerName(), owner);
        if (claimed == 1) {
            // 已重放记录、同持有人重入或过期租约均可原子进入新一轮分派。
            return true;
        }

        String status = jdbc.queryForObject("""
                SELECT status FROM outboxpro_dead_letter
                WHERE event_id = ? AND consumer_name = ?
                """, String.class, context.eventId(), context.consumerName());
        if (DeadLetterStatus.PENDING_REPLAY.name().equals(status)
                || DeadLetterStatus.REPLAYING.name().equals(status)) {
            // 已有可靠死信副本，重复 RabbitMQ 投递可以安全 ACK。
            return false;
        }

        // 活跃 DISPATCHING 不能当成“已可靠处理”，否则持有人崩溃后的重投递会被误 ACK。
        throw new IllegalStateException(
                "Dead letter dispatch is already in progress for event " + context.eventId()
                        + " and consumer " + context.consumerName());
    }

    /**
     * 将任意旧式分派记录转为待重放，并增加对应计数桶。
     *
     * @param eventId 原始事件 ID
     * @param consumerName 消费者名称
     * @param at 成功分派时间
     */
    @Override
    @Transactional
    public void markPendingReplay(String eventId, String consumerName, Instant at) {
        int updated = jdbc.update("""
                UPDATE outboxpro_dead_letter
                SET status = 'PENDING_REPLAY', dispatch_owner = NULL, dispatch_until = NULL,
                    updated_time = ?, version = version + 1
                WHERE event_id = ? AND consumer_name = ? AND status = 'DISPATCHING'
                """, Timestamp.from(at), eventId, consumerName);
        if (updated == 1) {
            // 只有首次从 DISPATCHING 迁移时增加计数，重复调用不会重复计数。
            changeCounter(bucket(eventId), 1);
        }
    }

    /**
     * 由当前租约持有人将死信记录转为待重放状态。
     *
     * @param eventId 原始事件 ID
     * @param consumerName 消费者名称
     * @param owner 当前分派租约持有人
     * @param at 成功分派时间
     * @return 当前持有人是否成功完成状态迁移
     */
    @Override
    @Transactional
    public boolean markPendingReplay(String eventId, String consumerName, String owner, Instant at) {
        int updated = jdbc.update("""
                UPDATE outboxpro_dead_letter
                SET status = 'PENDING_REPLAY', dispatch_owner = NULL, dispatch_until = NULL,
                    updated_time = ?, version = version + 1
                WHERE event_id = ? AND consumer_name = ?
                  AND status = 'DISPATCHING' AND dispatch_owner = ?
                """, Timestamp.from(at), eventId, consumerName, owner);
        if (updated == 1) {
            // 状态迁移和计数桶更新处于同一事务，保持监控计数精确。
            changeCounter(bucket(eventId), 1);
            return true;
        }
        return false;
    }

    /**
     * 释放当前持有人失败的分派租约。
     *
     * <p>记录保留在 {@code DISPATCHING}，但清空租约，使 RabbitMQ 的下一次重投递可以立即接管。</p>
     *
     * @param eventId 原始事件 ID
     * @param consumerName 消费者名称
     * @param owner 当前分派租约持有人
     */
    @Override
    @Transactional
    public void releaseDispatch(String eventId, String consumerName, String owner) {
        jdbc.update("""
                UPDATE outboxpro_dead_letter
                SET dispatch_owner = NULL, dispatch_until = NULL,
                    updated_time = NOW(6), version = version + 1
                WHERE event_id = ? AND consumer_name = ?
                  AND status = 'DISPATCHING' AND dispatch_owner = ?
                """, eventId, consumerName, owner);
    }

    /**
     * 按事件 ID 批量独占待重放记录并消耗重放次数。
     *
     * @param eventId 原始事件 ID
     * @param owner 重放租约持有人
     * @param maxReplayCount 单条记录最大重放次数
     * @param operator 审计操作人
     * @param reason 审计重放原因
     * @param at 认领时间
     * @return 当前请求成功认领的记录
     */
    @Override
    @Transactional
    public List<DeadLetterRecord> claimReplayByEventId(String eventId, String owner, int maxReplayCount,
                                                       String operator, String reason, Instant at) {
        List<DeadLetterRecord> candidates = jdbc.query("""
                SELECT id, event_id, event_type, consumer_name, queue_name,
                       original_exchange, original_routing_key, payload_json, attempt_count,
                       reason_code, reason_retryable, reason_retry_exhausted,
                       exception_type, exception_message, status, replay_count, replay_owner,
                       replayed_time, last_replay_operator, last_replay_reason, last_replay_error,
                       created_time, updated_time
                FROM outboxpro_dead_letter
                WHERE event_id = ? AND status = 'PENDING_REPLAY' AND replay_count < ?
                ORDER BY id
                FOR UPDATE
                """, this::map, eventId, maxReplayCount);
        if (candidates.isEmpty()) {
            return List.of();
        }

        // 先锁定候选行，再用一条 UPDATE 完成整个事件的重放认领和次数递增。
        String placeholders = candidates.stream().map(ignored -> "?").collect(Collectors.joining(","));
        String updateSql = """
                UPDATE outboxpro_dead_letter
                SET status = 'REPLAYING', replay_owner = ?, replay_count = replay_count + 1,
                    last_replay_operator = ?, last_replay_reason = ?, last_replay_error = NULL,
                    updated_time = ?, version = version + 1
                WHERE id IN (%s) AND status = 'PENDING_REPLAY' AND replay_count < ?
                """.formatted(placeholders);
        List<Object> args = new ArrayList<>(5 + candidates.size());
        args.add(owner);
        args.add(operator);
        args.add(reason);
        args.add(Timestamp.from(at));
        candidates.forEach(record -> args.add(record.id()));
        args.add(maxReplayCount);
        jdbc.update(updateSql, args.toArray());

        // 认领后记录不再属于待重放集合，因此只需一次性减少一个计数桶。
        changeCounter(bucket(eventId), -candidates.size());
        return candidates.stream()
                .map(record -> withReplayState(record, owner, operator, reason, at))
                .toList();
    }

    /**
     * 将目标端已确认的重放记录标记为 REPLAYED。
     *
     * @param id 死信台账主键
     * @param owner 当前重放租约持有人
     * @param at 确认时间
     */
    @Override
    public void markReplaySucceeded(long id, String owner, Instant at) {
        jdbc.update("""
                UPDATE outboxpro_dead_letter
                SET status = 'REPLAYED', replay_owner = NULL, replayed_time = ?,
                    updated_time = ?, version = version + 1
                WHERE id = ? AND status = 'REPLAYING' AND replay_owner = ?
                """, Timestamp.from(at), Timestamp.from(at), id, owner);
    }

    /**
     * 释放失败的重放租约，使记录回到待重放状态。
     *
     * @param id 死信台账主键
     * @param owner 当前重放租约持有人
     * @param error 已截断的失败信息
     * @param at 失败时间
     */
    @Override
    @Transactional
    public void releaseReplay(long id, String owner, String error, Instant at) {
        int updated = jdbc.update("""
                UPDATE outboxpro_dead_letter
                SET status = 'PENDING_REPLAY', replay_owner = NULL, last_replay_error = ?,
                    updated_time = ?, version = version + 1
                WHERE id = ? AND status = 'REPLAYING' AND replay_owner = ?
                """, truncate(error), Timestamp.from(at), id, owner);
        if (updated == 1) {
            // 只读取路由计数所需的 eventId，避免为一次失败释放构造完整死信对象。
            String eventId = jdbc.queryForObject("SELECT event_id FROM outboxpro_dead_letter WHERE id = ?",
                    String.class, id);
            changeCounter(bucket(eventId), 1);
        }
    }

    /**
     * 恢复超过一分钟未更新的重放租约，并把记录重新计入待重放计数。
     *
     * @param now 当前时间
     * @return 恢复的记录数量
     */
    @Override
    @Transactional
    public int recoverExpiredReplays(Instant now) {
        List<DeadLetterRecord> expired = jdbc.query("""
                SELECT id, event_id, event_type, consumer_name, queue_name,
                       original_exchange, original_routing_key, payload_json, attempt_count,
                       reason_code, reason_retryable, reason_retry_exhausted,
                       exception_type, exception_message, status, replay_count, replay_owner,
                       replayed_time, last_replay_operator, last_replay_reason, last_replay_error,
                       created_time, updated_time
                FROM outboxpro_dead_letter
                WHERE status = 'REPLAYING'
                  AND updated_time < DATE_SUB(?, INTERVAL 1 MINUTE)
                FOR UPDATE
                """, this::map, Timestamp.from(now));
        if (expired.isEmpty()) {
            return 0;
        }

        String placeholders = expired.stream().map(ignored -> "?").collect(Collectors.joining(","));
        List<Object> args = new ArrayList<>(expired.size() + 1);
        args.add(Timestamp.from(now));
        expired.forEach(record -> args.add(record.id()));
        jdbc.update("""
                UPDATE outboxpro_dead_letter
                SET status = 'PENDING_REPLAY', replay_owner = NULL,
                    last_replay_error = 'Replay lease expired and was recovered',
                    updated_time = ?, version = version + 1
                WHERE id IN (%s) AND status = 'REPLAYING'
                """.formatted(placeholders), args.toArray());

        // 按事件哈希聚合恢复数量，最多更新 counterBuckets 个桶，避免逐条写计数器。
        java.util.Map<Integer, Integer> bucketDeltas = new java.util.HashMap<>();
        expired.forEach(record -> bucketDeltas.merge(bucket(record.eventId()), 1, Integer::sum));
        bucketDeltas.forEach((bucket, count) -> changeCounter(bucket, count));
        return expired.size();
    }

    /**
     * 从固定数量的计数桶读取待重放总量。
     *
     * @return 待重放死信精确数量
     */
    @Override
    public long pendingReplayCount() {
        Long count = jdbc.queryForObject("SELECT COALESCE(SUM(pending_count), 0) FROM outboxpro_dead_letter_counter",
                Long.class);
        return count == null ? 0L : count;
    }

    /** 更新指定计数桶；数据库不存在桶时先创建，避免启动时必须预置固定行。 */
    private void changeCounter(int bucket, int delta) {
        jdbc.update("""
                INSERT INTO outboxpro_dead_letter_counter (counter_bucket, pending_count, version)
                VALUES (?, ?, 0)
                ON DUPLICATE KEY UPDATE pending_count = GREATEST(0, pending_count + ?),
                                        updated_time = NOW(), version = version + 1
                """, bucket, Math.max(0, delta), delta);
    }

    /** 使用稳定的非负哈希把事件分散到计数桶，减少单行写热点。 */
    private int bucket(String eventId) {
        return Math.floorMod(eventId.hashCode(), counterBuckets);
    }

    /** 将 JDBC 行映射为死信台账记录。 */
    private DeadLetterRecord map(ResultSet rs, int rowNum) throws SQLException {
        Timestamp replayed = rs.getTimestamp("replayed_time");
        Timestamp created = rs.getTimestamp("created_time");
        Timestamp updated = rs.getTimestamp("updated_time");
        DeadLetterReason reason = new DeadLetterReason(
                DeadLetterReasonCode.valueOf(rs.getString("reason_code")),
                rs.getBoolean("reason_retryable"), rs.getBoolean("reason_retry_exhausted"),
                rs.getString("exception_type"), rs.getString("exception_message"));
        return new DeadLetterRecord(rs.getLong("id"), rs.getString("event_id"), rs.getString("event_type"),
                rs.getString("consumer_name"), rs.getString("queue_name"), rs.getString("original_exchange"),
                rs.getString("original_routing_key"), rs.getString("payload_json"), rs.getInt("attempt_count"),
                reason, DeadLetterStatus.valueOf(rs.getString("status")), rs.getInt("replay_count"),
                rs.getString("replay_owner"), replayed == null ? null : replayed.toInstant(),
                rs.getString("last_replay_operator"), rs.getString("last_replay_reason"),
                rs.getString("last_replay_error"), created == null ? null : created.toInstant(),
                updated == null ? null : updated.toInstant());
    }

    /** 返回带有本次重放租约信息的内存记录。 */
    private DeadLetterRecord withReplayState(DeadLetterRecord record, String owner, String operator,
                                             String replayReason, Instant at) {
        return new DeadLetterRecord(record.id(), record.eventId(), record.eventType(), record.consumerName(),
                record.queue(), record.originalExchange(), record.originalRoutingKey(), record.payloadJson(),
                record.attemptCount(), record.reason(), DeadLetterStatus.REPLAYING, record.replayCount() + 1,
                owner, record.replayedAt(), operator, replayReason, null, record.createdAt(), at);
    }

    /** 限制异常和审计字段长度，避免异常文本导致数据库行膨胀。 */
    private String truncate(String value) {
        return value == null ? null : value.substring(0, Math.min(value.length(), 2000));
    }

    /**
     * 按条件分页检索死信台账。
     * 全部条件走参数绑定，状态为枚举安全转换，杜绝检索输入带来的注入风险。
     *
     * @param query 检索条件
     * @return 按 id 倒序排列的命中记录
     */
    @Override
    public List<DeadLetterRecord> findDeadLetters(org.outboxpro.spi.deadletter.DeadLetterQuery query) {
        List<Object> args = new ArrayList<>();
        String where = buildDeadLetterWhere(query, args);
        String sql = """
                SELECT id, event_id, event_type, consumer_name, queue_name,
                       original_exchange, original_routing_key, payload_json, attempt_count,
                       reason_code, reason_retryable, reason_retry_exhausted,
                       exception_type, exception_message, status, replay_count, replay_owner,
                       replayed_time, last_replay_operator, last_replay_reason, last_replay_error,
                       created_time, updated_time
                FROM outboxpro_dead_letter
                %s
                ORDER BY id DESC
                LIMIT ? OFFSET ?
                """.formatted(where);
        args.add(Math.min(query.limit(), MAX_QUERY_LIMIT));
        args.add(Math.max(query.offset(), 0));
        return jdbc.query(sql, this::map, args.toArray());
    }

    /**
     * 统计检索条件命中的死信总数。
     *
     * @param query 检索条件
     * @return 命中总数
     */
    @Override
    public long countDeadLetters(org.outboxpro.spi.deadletter.DeadLetterQuery query) {
        List<Object> args = new ArrayList<>();
        String where = buildDeadLetterWhere(query, args);
        String sql = "SELECT COUNT(*) FROM outboxpro_dead_letter %s".formatted(where);
        Long count = jdbc.queryForObject(sql, Long.class, args.toArray());
        return count == null ? 0L : count;
    }

    /** 运维检索单页行数上限，防止一次性拉取大结果集。 */
    private static final int MAX_QUERY_LIMIT = 200;

    /** 由检索条件构建动态 WHERE 子句并填充绑定参数。 */
    private String buildDeadLetterWhere(org.outboxpro.spi.deadletter.DeadLetterQuery query, List<Object> args) {
        StringBuilder where = new StringBuilder();
        if (query.eventType() != null && !query.eventType().isBlank()) {
            where.append("event_type = ?");
            args.add(query.eventType());
        }
        if (query.consumerName() != null && !query.consumerName().isBlank()) {
            appendWhereAnd(where);
            where.append("consumer_name = ?");
            args.add(query.consumerName());
        }
        if (query.status() != null) {
            appendWhereAnd(where);
            where.append("status = ?");
            args.add(query.status().name());
        }
        return where.isEmpty() ? "" : "WHERE " + where;
    }

    /** 向动态 WHERE 子句追加 AND 连接符。 */
    private void appendWhereAnd(StringBuilder where) {
        if (!where.isEmpty()) {
            where.append(" AND ");
        }
    }
}


