package org.outboxpro.autoconfigure;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

import java.util.Map;

/**
 * OutboxPro 积压健康检查。
 *
 * <p>健康检查读取与 Gauge 共享的 TTL 快照，不会因 Kubernetes 探针频率或 Prometheus
 * 抓取频率放大数据库压力。DEAD 与待人工重放死信代表需要人工介入的终态，超过阈值时
 * 返回 DOWN；PENDING / RETRY_WAITING 积压代表投递延迟，超过阈值时返回 OUT_OF_SERVICE。</p>
 */
public final class OutboxProHealthIndicator implements HealthIndicator {
    private final OutboxProObservabilitySnapshot snapshot;
    private final OutboxProProperties.Metrics.Health config;

    /**
     * 创建健康检查。
     *
     * @param snapshot 共享积压快照
     * @param config 观测配置
     */
    public OutboxProHealthIndicator(OutboxProObservabilitySnapshot snapshot,
                                    OutboxProProperties.Metrics.Health config) {
        this.snapshot = snapshot;
        this.config = config;
    }

    @Override
    public Health health() {
        OutboxProObservabilitySnapshot.OutboxSnapshot current = snapshot.get();
        Long dead = current.outboxCount("DEAD");
        Long pending = current.outboxCount("PENDING");
        Long retryWaiting = current.outboxCount("RETRY_WAITING");
        Long processing = current.outboxCount("PROCESSING");
        Long dlqPending = current.pendingReplayCount();

        Health.Builder builder;
        if (current.outboxQueryFailed() || current.dlqQueryFailed()) {
            builder = Health.down();
        } else if (dead != null && dead >= config.getOutboxDeadThreshold()
                || dlqPending != null && dlqPending >= config.getDlqPendingThreshold()) {
            builder = Health.down();
        } else if (pending != null && retryWaiting != null
                && pending + retryWaiting >= config.getOutboxBacklogThreshold()) {
            builder = Health.outOfService();
        } else {
            builder = Health.up();
        }

        builder.withDetail("outboxPending", valueOrUnknown(pending));
        builder.withDetail("outboxProcessing", valueOrUnknown(processing));
        builder.withDetail("outboxRetryWaiting", valueOrUnknown(retryWaiting));
        builder.withDetail("outboxDead", valueOrUnknown(dead));
        builder.withDetail("dlqPendingReplay", valueOrUnknown(dlqPending));
        builder.withDetail("snapshotCapturedAt", current.capturedAt().toString());
        builder.withDetail("outboxStatsAvailable", !current.outboxByStatus().isEmpty());
        builder.withDetail("dlqStatsAvailable", !current.pendingReplayByReason().isEmpty());
        builder.withDetail("outboxQueryFailed", current.outboxQueryFailed());
        builder.withDetail("dlqQueryFailed", current.dlqQueryFailed());
        if (!current.pendingReplayByReason().isEmpty()) {
            builder.withDetail("dlqPendingByReason", current.pendingReplayByReason());
        }
        return builder.build();
    }

    private Object valueOrUnknown(Long value) {
        return value == null ? "unknown" : value;
    }
}
