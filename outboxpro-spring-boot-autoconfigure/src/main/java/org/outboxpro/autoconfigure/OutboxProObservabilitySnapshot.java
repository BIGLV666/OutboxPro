package org.outboxpro.autoconfigure;

import org.outboxpro.spi.deadletter.DeadLetterRepository;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * OutboxPro 积压观测快照，供 Micrometer Gauge 与 Actuator 健康检查共享。
 *
 * <p>多个监控读取方可能同时到达（Prometheus 抓取、Kubernetes 探针、管理台刷新）。
 * 快照按 TTL 缓存，并用单一引用 CAS 保证同一时间最多一次数据库查询；
 * 查询失败返回带时间戳的空快照，不会向监控调用方传播异常。</p>
 */
public class OutboxProObservabilitySnapshot {
    private static final Logger log = LoggerFactory.getLogger(OutboxProObservabilitySnapshot.class);

    private final OutboxRepository outboxRepository;
    private final DeadLetterRepository deadLetterRepository;
    private final Duration ttl;
    private final AtomicReference<Cached> cached =
            new AtomicReference<>(new Cached(OutboxSnapshot.unsupported(), Instant.EPOCH));

    /**
     * 创建快照读取器。
     *
     * @param outboxRepository Outbox 仓储
     * @param deadLetterRepository 死信台账仓储；台账关闭时为 null
     * @param ttl 快照缓存时长
     */
    public OutboxProObservabilitySnapshot(OutboxRepository outboxRepository,
                                          DeadLetterRepository deadLetterRepository,
                                          Duration ttl) {
        if (ttl == null || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("snapshot ttl must be positive");
        }
        this.outboxRepository = outboxRepository;
        this.deadLetterRepository = deadLetterRepository;
        this.ttl = ttl;
    }

    /**
     * 获取当前快照；过期时触发一次节流刷新。
     *
     * @return 当前积压快照
     */
    public OutboxSnapshot get() {
        Cached current = cached.get();
        Instant now = Instant.now();
        if (now.isBefore(current.expiresAt)) {
            return current.value;
        }
        // CAS 只提交缓存结果；当前返回值可能被并发线程覆盖，但不会放大数据库查询。
        Cached next = new Cached(load(now), now.plus(ttl));
        cached.compareAndSet(current, next);
        return cached.get().value;
    }

    /** 只读取缓存，不触发数据库查询；用于避免监控侧放大故障流量。 */
    public OutboxSnapshot peek() {
        return cached.get().value;
    }

    private OutboxSnapshot load(Instant now) {
        Map<String, Long> outbox;
        Long oldestPendingAgeSeconds = null;
        boolean outboxQueryFailed = false;
        try {
            outbox = outboxRepository == null ? Map.of() : outboxRepository.countByStatus();
            oldestPendingAgeSeconds = outboxRepository == null ? null : outboxRepository.oldestPendingAgeSeconds(now);
        } catch (RuntimeException error) {
            log.warn("Outbox backlog snapshot query failed", error);
            outbox = Map.of();
            oldestPendingAgeSeconds = null;
            outboxQueryFailed = true;
        }

        Map<String, Long> dlq;
        boolean dlqQueryFailed = false;
        try {
            dlq = deadLetterRepository == null ? Map.of() : deadLetterRepository.pendingReplayCountByReason();
        } catch (RuntimeException error) {
            log.warn("Dead letter backlog snapshot query failed", error);
            dlq = Map.of();
            dlqQueryFailed = true;
        }
        return new OutboxSnapshot(outbox, dlq, oldestPendingAgeSeconds, outboxQueryFailed, dlqQueryFailed, now);
    }

    /** 当前框架积压快照。 */
    public record OutboxSnapshot(Map<String, Long> outboxByStatus, Map<String, Long> pendingReplayByReason,
                                 Long oldestPendingAgeSeconds, boolean outboxQueryFailed, boolean dlqQueryFailed,
                                 Instant capturedAt) {
        static OutboxSnapshot unsupported() {
            return new OutboxSnapshot(Map.of(), Map.of(), null, false, false, Instant.EPOCH);
        }

        /** @return 指定 Outbox 状态数量；不支持时返回 null。 */
        public Long outboxCount(String status) {
            return outboxByStatus.get(status);
        }

        /** @return 当前待人工重放总数；不支持时返回 null。 */
        public Long pendingReplayCount() {
            return pendingReplayByReason.isEmpty() ? null : pendingReplayByReason.values().stream()
                    .mapToLong(Long::longValue).sum();
        }

        /** @return 待重放原因分布；不支持时返回空 Map。 */
        public Map<String, Long> pendingReplayByReason() {
            return pendingReplayByReason;
        }

        /** @return Outbox 状态分布；不支持时返回空 Map。 */
        public Map<String, Long> outboxByStatus() {
            return outboxByStatus;
        }

        /** @return 最早待投递消息的等待秒数；无积压为 0，不支持时为 null。 */
        public Long oldestPendingAgeSeconds() {
            return oldestPendingAgeSeconds;
        }
    }

    private record Cached(OutboxSnapshot value, Instant expiresAt) { }
}
