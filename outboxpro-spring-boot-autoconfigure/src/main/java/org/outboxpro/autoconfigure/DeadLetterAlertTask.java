package org.outboxpro.autoconfigure;

import org.outboxpro.spi.deadletter.DeadLetterAlertNotifier;
import org.outboxpro.spi.deadletter.DeadLetterRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Duration;
import java.time.Instant;

/**
 * 死信告警监控任务。
 *
 * <p>周期性读取分桶计数器的待重放总量，当积压超过阈值时触发告警，
 * 当积压降低至恢复阈值以下且经过冷却时长后发送恢复通知。</p>
 *
 * <p>读取成本固定，不随死信台账数据膨胀而增长。</p>
 */
public final class DeadLetterAlertTask {
    private static final Logger log = LoggerFactory.getLogger(DeadLetterAlertTask.class);

    private final DeadLetterRepository deadLetterRepository;
    private final DeadLetterAlertNotifier alertNotifier;
    private final long threshold;
    private final long recoveryThreshold;
    private final Duration cooldown;

    private boolean alertFired = false;
    private Instant lastAlertTime = Instant.EPOCH;

    /**
     * 创建死信告警任务。
     *
     * @param deadLetterRepository 死信台账仓储
     * @param alertNotifier 告警通知器
     * @param threshold 触发高位告警的待重放数量阈值
     * @param recoveryThreshold 触发恢复通知的待重放数量阈值
     * @param cooldown 告警冷却时长
     */
    public DeadLetterAlertTask(DeadLetterRepository deadLetterRepository,
                               DeadLetterAlertNotifier alertNotifier,
                               long threshold,
                               long recoveryThreshold,
                               Duration cooldown) {
        if (recoveryThreshold >= threshold) {
            throw new IllegalArgumentException("recoveryThreshold must be less than threshold");
        }
        this.deadLetterRepository = deadLetterRepository;
        this.alertNotifier = alertNotifier;
        this.threshold = threshold;
        this.recoveryThreshold = recoveryThreshold;
        this.cooldown = cooldown;
    }

    /**
     * 周期性检查死信积压并触发告警或恢复通知。
     *
     * <p>调度间隔由配置属性 {@code outboxpro.dlq.alert.poll-interval} 控制。</p>
     */
    @Scheduled(fixedDelayString = "${outboxpro.dlq.alert.poll-interval:60000}")
    public void checkDeadLetterBacklog() {
        try {
            // 告警读取前恢复超时重放租约，避免崩溃记录从积压统计中永久消失。
            deadLetterRepository.recoverExpiredReplays(Instant.now());
            long pending = deadLetterRepository.pendingReplayCount();

            if (pending >= threshold && !alertFired) {
                // 待重放数量达到阈值且告警未触发，发送告警通知
                alertNotifier.onHighWatermark(pending, threshold);
                alertFired = true;
                lastAlertTime = Instant.now();
                log.warn("Dead letter backlog reached threshold: pending={}, threshold={}", pending, threshold);
            } else if (pending < recoveryThreshold && alertFired) {
                // 待重放数量降低至恢复阈值以下且告警已触发，判断是否满足冷却时长
                if (Instant.now().isAfter(lastAlertTime.plus(cooldown))) {
                    alertNotifier.onRecovered(pending, recoveryThreshold);
                    alertFired = false;
                    log.info("Dead letter backlog recovered: pending={}, recoveryThreshold={}", pending, recoveryThreshold);
                }
            }
        } catch (RuntimeException error) {
            // 告警任务失败不应影响业务消息处理，记录日志后吞掉异常
            log.error("Dead letter alert task failed", error);
        }
    }
}
