package org.outboxpro.autoconfigure;

import jakarta.annotation.PostConstruct;
import org.outboxpro.spi.deadletter.DeadLetterHandlingMode;

import java.time.Duration;

/**
 * 死信队列 (DLQ) 配置属性。
 *
 * <p>控制死信台账、重放接口和告警监控的开关与参数。
 * 重放功能依赖台账，配置冲突时启动快速失败。</p>
 */
public class DeadLetterQueueProperties {
    private DeadLetterHandlingMode handlingMode = DeadLetterHandlingMode.FRAMEWORK;
    private Ledger ledger = new Ledger();
    private Replay replay = new Replay();
    private Alert alert = new Alert();

    /**
     * 返回死信处理模式。
     *
     * @return 框架发布或用户完全接管
     */
    public DeadLetterHandlingMode getHandlingMode() {
        return handlingMode;
    }

    /** 设置死信处理模式。 */
    public void setHandlingMode(DeadLetterHandlingMode handlingMode) {
        this.handlingMode = handlingMode;
    }

    /** 返回台账配置。 */
    public Ledger getLedger() {
        return ledger;
    }

    /** 设置台账配置。 */
    public void setLedger(Ledger ledger) {
        this.ledger = ledger;
    }

    /** 返回重放配置。 */
    public Replay getReplay() {
        return replay;
    }

    /** 设置重放配置。 */
    public void setReplay(Replay replay) {
        this.replay = replay;
    }

    /** 返回告警配置。 */
    public Alert getAlert() {
        return alert;
    }

    /** 设置告警配置。 */
    public void setAlert(Alert alert) {
        this.alert = alert;
    }

    /**
     * 校验配置约束：重放功能必须依赖台账。
     *
     * @throws IllegalStateException 当重放启用但台账关闭时抛出
     */
    @PostConstruct
    public void validate() {
        if (handlingMode == null) {
            throw new IllegalStateException("outboxpro.dlq.handling-mode must not be null");
        }
        if (ledger == null || replay == null || alert == null) {
            throw new IllegalStateException("outboxpro.dlq.ledger/replay/alert must not be null");
        }
        if (replay.enabled && !ledger.enabled) {
            throw new IllegalStateException(
                    "outboxpro.dlq.replay.enabled=true requires outboxpro.dlq.ledger.enabled=true. " +
                    "Replay needs the ledger to provide original messages, routing, and audit trail."
            );
        }
        if (ledger.counterBuckets <= 0) {
            throw new IllegalStateException("outboxpro.dlq.ledger.counter-buckets must be positive");
        }
        if (ledger.maxReplayCount < 1) {
            throw new IllegalStateException("outboxpro.dlq.ledger.max-replay-count must be positive");
        }
        if (alert.pollInterval == null || alert.pollInterval.isNegative() || alert.pollInterval.isZero()) {
            throw new IllegalStateException("outboxpro.dlq.alert.poll-interval must be positive");
        }
        if (alert.threshold <= 0 || alert.recoveryThreshold < 0 || alert.recoveryThreshold >= alert.threshold) {
            throw new IllegalStateException(
                    "outboxpro.dlq.alert requires threshold > recovery-threshold >= 0 and threshold > 0");
        }
        if (alert.cooldown == null || alert.cooldown.isNegative()) {
            throw new IllegalStateException("outboxpro.dlq.alert.cooldown must not be negative");
        }
    }

    /**
     * 死信台账配置。
     */
    public static class Ledger {
        private boolean enabled = true;
        private boolean schemaInitialize = true;
        private int counterBuckets = 32;
        private int maxReplayCount = 3;

        /** 返回是否启用死信台账。 */
        public boolean isEnabled() {
            return enabled;
        }

        /** 设置是否启用死信台账。 */
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        /**
         * 返回是否自动执行死信台账 DDL。
         */
        public boolean isSchemaInitialize() {
            return schemaInitialize;
        }

        /** 设置是否自动执行死信台账 DDL。 */
        public void setSchemaInitialize(boolean schemaInitialize) {
            this.schemaInitialize = schemaInitialize;
        }

        /**
         * 返回分桶计数器桶数。
         *
         * <p>桶数越多，并发写入热点越分散，但告警扫描成本固定。建议 16-64 之间。</p>
         */
        public int getCounterBuckets() {
            return counterBuckets;
        }

        /** 设置分桶计数器桶数。 */
        public void setCounterBuckets(int counterBuckets) {
            this.counterBuckets = counterBuckets;
        }

        /**
         * 返回单条死信记录的最大重放次数。
         *
         * <p>达到此上限后，重放接口拒绝继续认领该记录。</p>
         */
        public int getMaxReplayCount() {
            return maxReplayCount;
        }

        /** 设置单条死信记录的最大重放次数。 */
        public void setMaxReplayCount(int maxReplayCount) {
            this.maxReplayCount = maxReplayCount;
        }
    }

    /**
     * 死信重放接口配置。
     */
    public static class Replay {
        private boolean enabled = false;

        /**
         * 返回是否暴露 HTTP 重放端点。
         *
         * <p>默认关闭，避免未授权的业务副作用。</p>
         */
        public boolean isEnabled() {
            return enabled;
        }

        /** 设置是否暴露 HTTP 重放端点。 */
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    /**
     * 死信告警配置。
     */
    public static class Alert {
        private boolean enabled = true;
        private Duration pollInterval = Duration.ofMinutes(1);
        private long threshold = 100;
        private long recoveryThreshold = 80;
        private Duration cooldown = Duration.ofMinutes(15);

        /** 返回是否启用死信告警。 */
        public boolean isEnabled() {
            return enabled;
        }

        /** 设置是否启用死信告警。 */
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        /**
         * 返回告警轮询间隔。
         */
        public Duration getPollInterval() {
            return pollInterval;
        }

        /** 设置告警轮询间隔。 */
        public void setPollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
        }

        /**
         * 返回触发高位告警的待重放数量阈值。
         */
        public long getThreshold() {
            return threshold;
        }

        /** 设置触发高位告警的待重放数量阈值。 */
        public void setThreshold(long threshold) {
            this.threshold = threshold;
        }

        /**
         * 返回触发恢复通知的待重放数量阈值。
         */
        public long getRecoveryThreshold() {
            return recoveryThreshold;
        }

        /** 设置触发恢复通知的待重放数量阈值。 */
        public void setRecoveryThreshold(long recoveryThreshold) {
            this.recoveryThreshold = recoveryThreshold;
        }

        /**
         * 返回告警冷却时长。
         *
         * <p>告警触发后必须经过此时长且数量低于恢复阈值才会发送恢复通知。</p>
         */
        public Duration getCooldown() {
            return cooldown;
        }

        /** 设置告警冷却时长。 */
        public void setCooldown(Duration cooldown) {
            this.cooldown = cooldown;
        }
    }
}

