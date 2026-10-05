package org.outboxpro.autoconfigure;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.outboxpro.spi.deadletter.DeadLetterReasonCode;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

/**
 * OutboxPro 积压 Gauge 的 Micrometer 注册器。
 *
 * <p>Gauge 只读取共享 TTL 快照，注册后自身没有定时任务。原因码是框架稳定枚举，
 * 未知值统一收敛为 UNKNOWN，保证标签基数固定。</p>
 */
public final class MicrometerOutboxProBacklogMetrics {
    private static final Map<String, String> OUTBOX_GAUGES = Map.of(
            "pending", "outboxpro.outbox.backlog.pending",
            "processing", "outboxpro.outbox.backlog.processing",
            "retry_waiting", "outboxpro.outbox.backlog.retry_waiting",
            "dead", "outboxpro.outbox.backlog.dead");

    private final OutboxProObservabilitySnapshot snapshot;
    private final MeterRegistry registry;

    /**
     * 创建并注册积压 Gauge。
     *
     * @param snapshot 共享积压快照
     * @param registry Micrometer 注册中心
     */
    public MicrometerOutboxProBacklogMetrics(OutboxProObservabilitySnapshot snapshot, MeterRegistry registry) {
        this.snapshot = snapshot;
        this.registry = registry;
        register();
    }

    private void register() {
        OUTBOX_GAUGES.forEach((status, meter) -> Gauge.builder(meter, snapshot,
                        value -> nonNegative(value.get().outboxCount(status.toUpperCase(Locale.ROOT))))
                .strongReference(true)
                .register(registry));
        Gauge.builder("outboxpro.dlq.backlog.pending_replay", snapshot,
                        value -> nonNegative(value.get().pendingReplayCount()))
                .strongReference(true)
                .register(registry);
        for (String reason : gaugeReasons()) {
            Gauge.builder("outboxpro.dlq.backlog.by_reason", snapshot,
                            value -> nonNegative(value.get().pendingReplayByReason().get(reason)))
                    .tags(Tags.of(Tag.of("reason", reason)))
                    .strongReference(true)
                    .register(registry);
        }
    }

    /**
     * 为兼容历史/异常数据补充固定 UNKNOWN 标签。
     *
     * @return 当前稳定 reason 集合
     */
    private List<String> gaugeReasons() {
        return Stream.concat(Stream.of("UNKNOWN"),
                        Arrays.stream(DeadLetterReasonCode.values()).map(Enum::name).sorted())
                .distinct()
                .toList();
    }

    private double nonNegative(Long value) {
        return value == null ? 0.0 : Math.max(0.0, value.doubleValue());
    }
}
