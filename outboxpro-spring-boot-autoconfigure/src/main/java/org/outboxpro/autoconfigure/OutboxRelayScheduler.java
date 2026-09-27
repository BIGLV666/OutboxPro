package org.outboxpro.autoconfigure;

import org.outboxpro.persistence.OutboxRelay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Outbox Relay 定时调度器，负责周期性触发投递中继。
 *
 * <p>使用框架自有的单线程调度池而不是 Spring 的 {@code @Scheduled}：
 * Spring 默认调度器是单线程的，Relay 一批串行发布可能长达数十秒，
 * 会饿死同一调度器上的死信告警任务和业务应用自己的定时任务。
 * 框架任务跑在自己的线程上，与宿主应用的调度互不干扰。</p>
 */
public final class OutboxRelayScheduler {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelayScheduler.class);

    private final OutboxRelay relay;
    private final Duration pollInterval;
    private ScheduledExecutorService executor;

    /**
     * 创建调度器。
     *
     * @param relay 投递中继
     * @param pollInterval 触发间隔；单次执行结束到下次触发的固定延迟
     */
    public OutboxRelayScheduler(OutboxRelay relay, Duration pollInterval) {
        this.relay = relay;
        this.pollInterval = pollInterval;
    }

    /** 启动后台调度线程；首轮立即触发，与旧版 {@code @Scheduled} 首轮立即执行的语义保持一致。 */
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "outboxpro-relay");
            thread.setDaemon(true);
            return thread;
        });
        // fixedDelay：上一轮完全结束（含全部 Confirm 等待）后再计时，保证 relayOnce 不与自身重叠。
        executor.scheduleWithFixedDelay(this::relaySafely, 0, pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** 单轮异常不允许杀死调度线程；记录后等待下一轮，由租约过期机制兜底恢复卡住的记录。 */
    private void relaySafely() {
        try {
            relay.relayOnce();
        } catch (RuntimeException error) {
            log.warn("Outbox relay cycle failed; will retry on next tick", error);
        }
    }

    /** 停止调度线程并等待在途一轮结束。 */
    public void stop() {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
        executor = null;
    }
}
