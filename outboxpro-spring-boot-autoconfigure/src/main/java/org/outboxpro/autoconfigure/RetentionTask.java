package org.outboxpro.autoconfigure;

import org.outboxpro.spi.deadletter.DeadLetterRepository;
import org.outboxpro.spi.persistence.InboxRepository;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 历史数据保留任务：周期性分批清理框架表中可清理的终态行。
 *
 * <p>不清理会导致 outboxpro_outbox 的 SENT、outboxpro_inbox 的 SUCCESS、
 * outboxpro_message_log 全部记录以及死信台账的 REPLAYED 无限增长，
 * 拖慢 Relay 认领查询并膨胀存储。清理范围严格限定在终态：</p>
 * <ul>
 *   <li>Outbox：只清理 {@code SENT}（DEAD 等待人工重放，RETRY_WAITING 等待投递）；</li>
 *   <li>Inbox：只清理 {@code SUCCESS}（FAILED/IGNORED/RECEIVED 是幂等重开与排障依据）；</li>
 *   <li>消息日志：按发生时间清理；</li>
 *   <li>死信台账：只清理 {@code REPLAYED}（不触碰分派租约状态机与待重放计数桶）。</li>
 * </ul>
 *
 * <p><b>锁表防护（性能优先）</b>：每类删除都是 {@code DELETE ... ORDER BY id LIMIT ?} 的
 * 单语句短事务（自动提交），按主键序扫描、删够 LIMIT 立即返回，单语句持锁行数恒等于
 * 批大小；每轮受 max-rows-per-cycle 约束，大存量首次清理也只会推进有限行数，
 * 不产生长事务。定时与手动共用 {@link #purge()}，运行中互斥（后来的调用直接跳过，
 * 等待下一轮），绝不并发全表清理。</p>
 *
 * <p>{@code outboxpro.retention.enabled=false} 只关闭定时调度；手动清理端点仍可调用
 * {@link #purge()} 按需执行。</p>
 */
public final class RetentionTask {
    private static final Logger log = LoggerFactory.getLogger(RetentionTask.class);

    private final OutboxRepository outboxRepository;
    private final InboxRepository inboxRepository;
    private final DeadLetterRepository deadLetterRepository;
    private final JdbcTemplate jdbcTemplate;
    private final OutboxProProperties.Retention config;
    /** 运行互斥守卫：定时与手动清理不并发执行，避免不必要的锁竞争。 */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * 创建保留任务。
     *
     * @param outboxRepository Outbox 仓储
     * @param inboxRepository Inbox 仓储
     * @param deadLetterRepository 死信台账仓储；台账关闭时为 {@code null}
     * @param jdbcTemplate 消息日志表直接走 JDBC（该表为框架私有表，无独立仓储）
     * @param config 保留策略配置
     */
    public RetentionTask(OutboxRepository outboxRepository,
                         InboxRepository inboxRepository,
                         DeadLetterRepository deadLetterRepository,
                         JdbcTemplate jdbcTemplate,
                         OutboxProProperties.Retention config) {
        this.outboxRepository = outboxRepository;
        this.inboxRepository = inboxRepository;
        this.deadLetterRepository = deadLetterRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.config = config;
    }

    /**
     * 每小时触发一轮清理；首轮延迟 10 分钟，避开启动高峰（连接池预热、业务流量回升），
     * 也不与应用启动阶段的其他 DDL/迁移争抢数据库资源。调度异常由 Spring 记录，不影响下一轮。
     */
    @Scheduled(fixedDelay = 3600_000, initialDelay = 600_000)
    public void scheduledPurge() {
        PurgeResult result = purge();
        if (!result.skipped()) {
            log.info("Retention purge cycle finished: outboxSent={}, inboxSuccess={}, messageLog={}, deadLetterReplayed={}",
                    result.outboxSent(), result.inboxSuccess(), result.messageLog(), result.deadLetterReplayed());
        }
    }

    /**
     * 执行一轮清理并返回各类实际删除行数。
     * 供定时调度与运维端点的手动清理共用；运行中重复调用会返回 skipped=true。
     * 单类失败只记日志跳过（该类计 0），不影响其他类。
     *
     * @return 清理结果汇总
     */
    public PurgeResult purge() {
        if (!config.isEnabled()) {
            return PurgeResult.disabled();
        }
        if (!running.compareAndSet(false, true)) {
            // 上一轮尚未结束：跳过本轮而不是叠加锁竞争，等下一轮即可。
            log.info("Retention purge is already running; skipped this trigger");
            return PurgeResult.alreadyRunning();
        }
        try {
            long outbox = purgeOutbox();
            long inbox = purgeInbox();
            long messageLog = purgeMessageLog();
            long deadLetter = purgeDeadLetters();
            return new PurgeResult(outbox, inbox, messageLog, deadLetter, false);
        } finally {
            running.set(false);
        }
    }

    /** 分批清理 SENT 记录，单轮最多删除 max-rows-per-cycle 行。 */
    private long purgeOutbox() {
        try {
            return purgeByLimit(config.getOutboxSent(),
                    (cutoff, limit) -> outboxRepository.purgeSentBefore(cutoff, limit));
        } catch (RuntimeException error) {
            log.warn("Retention purge for outbox failed; skipped this cycle", error);
            return 0;
        }
    }

    /** 分批清理 Inbox SUCCESS 记录。 */
    private long purgeInbox() {
        try {
            return purgeByLimit(config.getInboxSuccess(),
                    (cutoff, limit) -> inboxRepository.purgeSuccessBefore(cutoff, limit));
        } catch (RuntimeException error) {
            log.warn("Retention purge for inbox failed; skipped this cycle", error);
            return 0;
        }
    }

    /** 分批清理消息日志记录。 */
    private long purgeMessageLog() {
        try {
            return purgeByLimit(config.getMessageLog(), (cutoff, limit) -> jdbcTemplate.update(
                    "DELETE FROM outboxpro_message_log WHERE occurred_time < ? ORDER BY id LIMIT ?",
                    java.sql.Timestamp.from(cutoff), limit));
        } catch (RuntimeException error) {
            log.warn("Retention purge for message log failed; skipped this cycle", error);
            return 0;
        }
    }

    /** 分批清理死信台账 REPLAYED 记录；台账未启用时直接跳过。 */
    private long purgeDeadLetters() {
        if (deadLetterRepository == null) {
            return 0;
        }
        try {
            return purgeByLimit(config.getDeadLetterReplayed(),
                    (cutoff, limit) -> deadLetterRepository.purgeReplayedBefore(cutoff, limit));
        } catch (RuntimeException error) {
            log.warn("Retention purge for dead letter ledger failed; skipped this cycle", error);
            return 0;
        }
    }

    /**
     * 通用分批删除循环：按批大小反复调用删除，直到该类清完或达到单轮上限。
     *
     * @param retention 保留时长
     * @param deleter 单批删除操作，返回实际删除行数
     * @return 本轮删除总行数
     */
    private long purgeByLimit(Duration retention, PurgeOperation deleter) {
        Instant cutoff = Instant.now().minus(retention);
        long total = 0;
        while (total < config.getMaxRowsPerCycle()) {
            int limit = Math.min(config.getBatchSize(), config.getMaxRowsPerCycle() - (int) total);
            int deleted = deleter.purge(cutoff, limit);
            total += deleted;
            if (deleted < limit) {
                break;
            }
        }
        return total;
    }

    /** 一轮清理的结果汇总。 */
    public record PurgeResult(long outboxSent, long inboxSuccess, long messageLog,
                              long deadLetterReplayed, boolean skipped) {

        /** @return 保留策略关闭时的空结果。 */
        public static PurgeResult disabled() {
            return new PurgeResult(0, 0, 0, 0, true);
        }

        /** @return 已有清理在运行时的跳过结果。 */
        public static PurgeResult alreadyRunning() {
            return new PurgeResult(0, 0, 0, 0, true);
        }
    }

    /** 单批删除操作契约。 */
    @FunctionalInterface
    private interface PurgeOperation {
        /**
         * 删除一批过期行。
         *
         * @param cutoff 截止时间
         * @param limit 本批上限
         * @return 实际删除行数
         */
        int purge(Instant cutoff, int limit);
    }
}
