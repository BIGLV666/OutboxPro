package org.outboxpro.autoconfigure;

import org.outboxpro.persistence.OutboxRelay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Outbox Relay 定时调度器，负责周期性触发投递中继。
 *
 * <p>使用框架自有的调度池而不是 Spring 的 {@code @Scheduled}：
 * Spring 默认调度器是单线程的，Relay 一批串行发布可能长达数十秒，
 * 会饿死同一调度器上的死信告警任务和业务应用自己的定时任务。
 * 框架任务跑在自己的线程上，与宿主应用的调度互不干扰。</p>
 *
 * <p>V1.4 起支持两种扩容手段：</p>
 * <ul>
 *   <li>{@code workers}：多个 Relay 工作线程并行认领，依赖数据库
 *       {@code FOR UPDATE SKIP LOCKED} 保证同一记录不会被并发认领；</li>
 *   <li>空闲退避：连续空轮时按指数增长轮询间隔，直到
 *       {@code idle-backoff-max}，有消息后立即恢复基础间隔，避免多实例空闲时
 *       把固定频率的轮询压力放大到数据库。</li>
 * </ul>
 */
public final class OutboxRelayScheduler {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelayScheduler.class);
    private static final int MAX_IDLE_SHIFT = 30;

    private final OutboxRelay relay;
    private final int workers;
    private final Duration pollInterval;
    private final Duration maxIdleInterval;
    private final AtomicInteger threadSequence = new AtomicInteger();
    private volatile ScheduledExecutorService executor;

    /**
     * 创建单工作线程、无空闲退避的调度器，保持 V1.3 及以前的行为。
     *
     * @param relay 投递中继
     * @param pollInterval 触发间隔
     */
    public OutboxRelayScheduler(OutboxRelay relay, Duration pollInterval) {
        this(relay, 1, pollInterval, pollInterval);
    }

    /**
     * 创建调度器。
     *
     * @param relay 投递中继
     * @param workers 并行工作线程数，必须为正
     * @param pollInterval 有消息时的基础轮询间隔
     * @param maxIdleInterval 连续空轮时轮询间隔的上限；小于基础间隔时按基础间隔执行
     */
    public OutboxRelayScheduler(OutboxRelay relay, int workers, Duration pollInterval, Duration maxIdleInterval) {
        if (workers < 1) {
            throw new IllegalArgumentException("workers must be positive");
        }
        if (pollInterval == null || pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("pollInterval must be positive");
        }
        Duration requestedMax = maxIdleInterval == null || maxIdleInterval.isNegative()
                ? pollInterval : maxIdleInterval;
        // 过期租约恢复发生在 Relay 轮询内；空闲退避若超过半个租约周期，崩溃实例
        // 遗留的 PROCESSING 记录会被推迟恢复。这里对退避上限做租约约束。
        long leaseBoundMillis = Math.max(1_000L, relay.claimTimeout().toMillis() / 2);
        long effectiveMaxMillis = Math.max(pollInterval.toMillis(),
                Math.min(requestedMax.toMillis(), leaseBoundMillis));
        this.relay = relay;
        this.workers = workers;
        this.pollInterval = pollInterval;
        this.maxIdleInterval = Duration.ofMillis(effectiveMaxMillis);
    }

    /**
     * 启动后台调度线程；每个工作线程首轮立即触发。
     */
    public synchronized void start() {
        if (executor != null) {
            return;
        }
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, "outboxpro-relay-" + threadSequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        ScheduledThreadPoolExecutor created = new ScheduledThreadPoolExecutor(workers, threadFactory);
        // shutdown 时取消尚未到期的延迟任务，避免 stop() 等待一个完整退避周期后
        // 再 shutdownNow() 中断正在发布的一轮。
        created.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        created.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor = created;
        for (int i = 0; i < workers; i++) {
            schedule(new Worker());
        }
    }

    /** 为单个工作线程安排下一轮；每轮结束后重新计算间隔，实现自适应退避。 */
    private void schedule(Worker worker) {
        ScheduledExecutorService current = executor;
        if (current == null || current.isShutdown()) {
            return;
        }
        long delayMillis = Math.max(1L, worker.nextDelay().toMillis());
        current.schedule(() -> runWorker(worker), delayMillis, TimeUnit.MILLISECONDS);
    }

    /** 单轮异常不允许杀死调度线程；记录后退避，由租约过期机制兜底恢复卡住的记录。 */
    private void runWorker(Worker worker) {
        boolean claimed;
        try {
            claimed = relay.relayOnceAndReportActivity();
        } catch (RuntimeException error) {
            log.warn("Outbox relay cycle failed; will retry after backoff", error);
            claimed = false;
        }
        // 异常按空轮处理：数据库或 Broker 故障时避免全速重试放大故障。
        if (claimed) {
            worker.resetIdle();
        } else {
            worker.recordIdle();
        }
        schedule(worker);
    }

    /**
     * 计算下一次轮询间隔：空轮次数每增加一次，间隔翻倍，直到上限。
     *
     * @param pollInterval 基础间隔
     * @param maxIdleInterval 最大间隔
     * @param idleStreak 连续空轮次数
     * @return 下一次轮询间隔，始终不小于基础间隔
     */
    static Duration nextDelay(Duration pollInterval, Duration maxIdleInterval, int idleStreak) {
        if (idleStreak <= 0) {
            return pollInterval;
        }
        long baseMillis = pollInterval.toMillis();
        long multiplier = 1L << Math.min(idleStreak, MAX_IDLE_SHIFT);
        long delayMillis = baseMillis > Long.MAX_VALUE / multiplier
                ? Long.MAX_VALUE : baseMillis * multiplier;
        long cappedMillis = Math.min(delayMillis, maxIdleInterval.toMillis());
        return Duration.ofMillis(Math.max(baseMillis, cappedMillis));
    }

    /** 停止全部调度线程并等待在途一轮结束。 */
    public synchronized void stop() {
        ScheduledExecutorService current = executor;
        executor = null;
        if (current == null) {
            return;
        }
        current.shutdown();
        try {
            if (!current.awaitTermination(10, TimeUnit.SECONDS)) {
                current.shutdownNow();
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            current.shutdownNow();
        }
    }

    /** 单个工作线程的空闲状态；每轮调度前计算下一次间隔。 */
    private final class Worker {
        private int idleStreak;
        private boolean first = true;

        private Duration nextDelay() {
            if (first) {
                first = false;
                return Duration.ZERO;
            }
            return OutboxRelayScheduler.nextDelay(pollInterval, maxIdleInterval, idleStreak);
        }

        private void resetIdle() {
            idleStreak = 0;
        }

        private void recordIdle() {
            if (idleStreak < MAX_IDLE_SHIFT) {
                idleStreak++;
            }
        }
    }
}
