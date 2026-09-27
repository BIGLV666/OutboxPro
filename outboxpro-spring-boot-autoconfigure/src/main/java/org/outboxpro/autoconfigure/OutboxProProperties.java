package org.outboxpro.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * OutboxPro 配置属性，配置前缀为 {@code outboxpro}。
 * 这些属性控制自动装配、事务 Outbox Relay、RabbitMQ 消费者、重试和消息日志。
 */
@ConfigurationProperties(prefix = "outboxpro")
public class OutboxProProperties {
    private boolean enabled = true;
    private String producerName = "application";
    private Producer producer = new Producer();
    private Consumer consumer = new Consumer();
    private Retry retry = new Retry();
    private Observability observability = new Observability();
    private DeadLetterQueueProperties dlq = new DeadLetterQueueProperties();
    private boolean schemaInitialize = true;
    private Ops ops = new Ops();
    private Retention retention = new Retention();

    /** 返回是否启用 OutboxPro 自动装配。 */
    public boolean isEnabled() { return enabled; }
    /** 设置是否启用 OutboxPro 自动装配。 */
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    /** 返回生产者服务名称，写入事件 Envelope 的 producer 字段。 */
    public String getProducerName() { return producerName; }
    /** 设置生产者服务名称。 */
    public void setProducerName(String producerName) { this.producerName = producerName; }
    /** 返回生产端配置。 */
    public Producer getProducer() { return producer; }
    /** 设置生产端配置。 */
    public void setProducer(Producer producer) { this.producer = producer; }
    /** 返回消费端配置。 */
    public Consumer getConsumer() { return consumer; }
    /** 设置消费端配置。 */
    public void setConsumer(Consumer consumer) { this.consumer = consumer; }
    /** 返回通用重试配置。 */
    public Retry getRetry() { return retry; }
    /** 设置通用重试配置。 */
    public void setRetry(Retry retry) { this.retry = retry; }
    /** 返回观测配置。 */
    public Observability getObservability() { return observability; }
    /** 设置观测配置。 */
    public void setObservability(Observability observability) { this.observability = observability; }
    /** 返回死信队列配置。 */
    public DeadLetterQueueProperties getDlq() { return dlq; }
    /** 设置死信队列配置。 */
    public void setDlq(DeadLetterQueueProperties dlq) { this.dlq = dlq; }
    /** 返回是否自动执行 MySQL OutboxPro DDL。 */
    public boolean isSchemaInitialize() { return schemaInitialize; }
    /** 设置是否自动执行 MySQL OutboxPro DDL。 */
    public void setSchemaInitialize(boolean schemaInitialize) { this.schemaInitialize = schemaInitialize; }
    /** 返回运维端点配置。 */
    public Ops getOps() { return ops; }
    /** 设置运维端点配置。 */
    public void setOps(Ops ops) { this.ops = ops; }
    /** 返回历史数据保留策略配置。 */
    public Retention getRetention() { return retention; }
    /** 设置历史数据保留策略配置。 */
    public void setRetention(Retention retention) { this.retention = retention; }

    /** 运维查询端点配置。 */
    public static class Ops {
        /** 默认关闭：检索端点会暴露消息与死信元数据，必须显式开启并配置授权器。 */
        private boolean enabled = false;

        /** 返回是否启用运维查询端点。 */
        public boolean isEnabled() { return enabled; }
        /** 设置是否启用运维查询端点。 */
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
    }

    /** 生产端 Relay 和 Publisher Confirm 配置。 */
    public static class Producer {
        private boolean enabled = true;
        private boolean relayEnabled = true;
        private int batchSize = 100;
        private Duration pollInterval = Duration.ofSeconds(1);
        private Duration claimTimeout = Duration.ofSeconds(60);
        private Duration confirmTimeout = Duration.ofSeconds(10);

        /** 返回是否启用生产端。 */
        public boolean isEnabled() { return enabled; }
        /** 设置是否启用生产端。 */
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        /** 返回是否启用定时 Relay。 */
        public boolean isRelayEnabled() { return relayEnabled; }
        /** 设置是否启用定时 Relay。 */
        public void setRelayEnabled(boolean relayEnabled) { this.relayEnabled = relayEnabled; }
        /** 返回单次 Relay 最大认领数量。 */
        public int getBatchSize() { return batchSize; }
        /** 设置单次 Relay 最大认领数量。 */
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
        /** 返回 Relay 轮询间隔。 */
        public Duration getPollInterval() { return pollInterval; }
        /** 设置 Relay 轮询间隔。 */
        public void setPollInterval(Duration pollInterval) { this.pollInterval = pollInterval; }
        /** 返回 Outbox 认领租约时长。 */
        public Duration getClaimTimeout() { return claimTimeout; }
        /** 设置 Outbox 认领租约时长。 */
        public void setClaimTimeout(Duration claimTimeout) { this.claimTimeout = claimTimeout; }
        /** 返回等待 RabbitMQ Publisher Confirm 的超时时间。 */
        public Duration getConfirmTimeout() { return confirmTimeout; }
        /** 设置等待 RabbitMQ Publisher Confirm 的超时时间。 */
        public void setConfirmTimeout(Duration confirmTimeout) { this.confirmTimeout = confirmTimeout; }
    }

    /** RabbitMQ Listener 并发和幂等配置。 */
    public static class Consumer {
        private boolean enabled = true;
        private int concurrency = 3;
        private int prefetch = 50;
        private boolean idempotencyEnabled = true;

        /** 返回是否启用消费者。 */
        public boolean isEnabled() { return enabled; }
        /** 设置是否启用消费者。 */
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        /** 返回每个队列的并发消费者数量。 */
        public int getConcurrency() { return concurrency; }
        /** 设置每个队列的并发消费者数量。 */
        public void setConcurrency(int concurrency) { this.concurrency = concurrency; }
        /** 返回 RabbitMQ prefetch 数量。 */
        public int getPrefetch() { return prefetch; }
        /** 设置 RabbitMQ prefetch 数量。 */
        public void setPrefetch(int prefetch) { this.prefetch = prefetch; }
        /** 返回是否启用 Inbox 幂等。 */
        public boolean isIdempotencyEnabled() { return idempotencyEnabled; }
        /** 设置是否启用 Inbox 幂等。V1 默认必须开启。 */
        public void setIdempotencyEnabled(boolean idempotencyEnabled) { this.idempotencyEnabled = idempotencyEnabled; }

        /**
         * 重试或死信转发失败、需要 NACK + requeue 保底时的重投递延迟。
         * 没有该延迟时，目标系统故障会让消息被 RabbitMQ 立即重投并全速热循环，
         * 消耗全部消费线程；默认 1s，把循环频率压到每消费线程每秒一次。
         */
        private Duration redeliveryDelay = Duration.ofSeconds(1);

        /**
         * Inbox RECEIVED 状态被视为孤儿记录的超时时间。
         * Best Effort 的 RECEIVED 记录在事务外提交，进程在 Handler 执行中途崩溃会留下
         * 永久 RECEIVED；超过该超时后重投递允许重开记录重新执行。默认 10 分钟，
         * 必须大于最坏 Handler 执行时长，否则并发重复投递可能双重执行。
         */
        private Duration inboxReceivedStaleTimeout = Duration.ofMinutes(10);

        /**
         * 全部订阅允许声明的 Retry Queue 总量上限（Σ 每个启用重试绑定的 maxAttempts-1）。
         * 订阅或重试档位过多会让 Broker 队列数量失控，启动时快速失败并给出收缩指引。
         */
        private int maxRetryQueueCount = 100;

        /** 返回 NACK + requeue 前的重投递延迟。 */
        public Duration getRedeliveryDelay() { return redeliveryDelay; }
        /** 设置 NACK + requeue 前的重投递延迟；0 表示立即重投（不推荐）。 */
        public void setRedeliveryDelay(Duration redeliveryDelay) { this.redeliveryDelay = redeliveryDelay; }
        /** 返回 RECEIVED 孤儿记录的重开超时。 */
        public Duration getInboxReceivedStaleTimeout() { return inboxReceivedStaleTimeout; }
        /** 设置 RECEIVED 孤儿记录的重开超时。 */
        public void setInboxReceivedStaleTimeout(Duration inboxReceivedStaleTimeout) {
            this.inboxReceivedStaleTimeout = inboxReceivedStaleTimeout;
        }
        /** 返回 Retry Queue 总量上限。 */
        public int getMaxRetryQueueCount() { return maxRetryQueueCount; }
        /** 设置 Retry Queue 总量上限。 */
        public void setMaxRetryQueueCount(int maxRetryQueueCount) { this.maxRetryQueueCount = maxRetryQueueCount; }
    }

    /** 历史数据保留策略配置：周期性分批清理框架三张表和死信台账中的可清理终态行。 */
    public static class Retention {
        /** 默认开启：不清理会导致 outbox SENT / inbox SUCCESS / message_log 无限增长。 */
        private boolean enabled = true;
        /**
         * Outbox 表 SENT 记录保留时长。
         * 默认 3 天：SENT 是终态，保留仅服务短期排障，保留越久 Relay 认领查询扫过的
         * 死行越多；配合保留任务每小时增量清理，表规模稳定在 3 天窗口内。
         */
        private Duration outboxSent = Duration.ofDays(3);
        /** Inbox 表 SUCCESS 记录保留时长。 */
        private Duration inboxSuccess = Duration.ofDays(30);
        /** 消息日志表记录保留时长。 */
        private Duration messageLog = Duration.ofDays(14);
        /** 死信台账 REPLAYED 记录保留时长。 */
        private Duration deadLetterReplayed = Duration.ofDays(30);
        /** 单次 DELETE 的批大小，避免长事务锁表。 */
        private int batchSize = 500;
        /** 每轮清理任务对单张表的最大删除行数上限。 */
        private int maxRowsPerCycle = 20000;

        /** 返回是否启用保留策略。 */
        public boolean isEnabled() { return enabled; }
        /** 设置是否启用保留策略。 */
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        /** 返回 Outbox SENT 记录保留时长。 */
        public Duration getOutboxSent() { return outboxSent; }
        /** 设置 Outbox SENT 记录保留时长。 */
        public void setOutboxSent(Duration outboxSent) { this.outboxSent = outboxSent; }
        /** 返回 Inbox SUCCESS 记录保留时长。 */
        public Duration getInboxSuccess() { return inboxSuccess; }
        /** 设置 Inbox SUCCESS 记录保留时长。 */
        public void setInboxSuccess(Duration inboxSuccess) { this.inboxSuccess = inboxSuccess; }
        /** 返回消息日志记录保留时长。 */
        public Duration getMessageLog() { return messageLog; }
        /** 设置消息日志记录保留时长。 */
        public void setMessageLog(Duration messageLog) { this.messageLog = messageLog; }
        /** 返回死信台账 REPLAYED 记录保留时长。 */
        public Duration getDeadLetterReplayed() { return deadLetterReplayed; }
        /** 设置死信台账 REPLAYED 记录保留时长。 */
        public void setDeadLetterReplayed(Duration deadLetterReplayed) { this.deadLetterReplayed = deadLetterReplayed; }
        /** 返回单次 DELETE 批大小。 */
        public int getBatchSize() { return batchSize; }
        /** 设置单次 DELETE 批大小。 */
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
        /** 返回每轮单表最大删除行数。 */
        public int getMaxRowsPerCycle() { return maxRowsPerCycle; }
        /** 设置每轮单表最大删除行数。 */
        public void setMaxRowsPerCycle(int maxRowsPerCycle) { this.maxRowsPerCycle = maxRowsPerCycle; }
    }

    /** 默认 Retry Queue 使用的退避配置。 */
    public static class Retry {
        private boolean enabled = true;
        private int maxAttempts = 5;
        private Duration initialDelay = Duration.ofSeconds(1);
        private double multiplier = 2;
        private Duration maxDelay = Duration.ofMinutes(5);

        /** 返回是否启用重试。 */
        public boolean isEnabled() { return enabled; }
        /** 设置是否启用重试。 */
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        /** 返回最大消费尝试次数。 */
        public int getMaxAttempts() { return maxAttempts; }
        /** 设置最大消费尝试次数。 */
        public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
        /** 返回初始重试延迟。 */
        public Duration getInitialDelay() { return initialDelay; }
        /** 设置初始重试延迟。 */
        public void setInitialDelay(Duration initialDelay) { this.initialDelay = initialDelay; }
        /** 返回退避倍增系数。 */
        public double getMultiplier() { return multiplier; }
        /** 设置退避倍增系数。 */
        public void setMultiplier(double multiplier) { this.multiplier = multiplier; }
        /** 返回最大重试延迟。 */
        public Duration getMaxDelay() { return maxDelay; }
        /** 设置最大重试延迟。 */
        public void setMaxDelay(Duration maxDelay) { this.maxDelay = maxDelay; }
    }

    /** 消息日志和观测配置。 */
    public static class Observability {
        private boolean enabled = true;
        private boolean messageLogEnabled = true;
        /** 消息日志 Sink 实现：slf4j（默认）或 database（异步批量写 outboxpro_message_log）。 */
        private String messageLogSink = "slf4j";
        private DbSink dbSink = new DbSink();

        /** 返回是否启用观测能力。 */
        public boolean isEnabled() { return enabled; }
        /** 设置是否启用观测能力。 */
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        /** 返回是否启用消息生命周期日志。 */
        public boolean isMessageLogEnabled() { return messageLogEnabled; }
        /** 设置是否启用消息生命周期日志。 */
        public void setMessageLogEnabled(boolean messageLogEnabled) { this.messageLogEnabled = messageLogEnabled; }
        /** 返回消息日志 Sink 类型。 */
        public String getMessageLogSink() { return messageLogSink; }
        /** 设置消息日志 Sink 类型。 */
        public void setMessageLogSink(String messageLogSink) { this.messageLogSink = messageLogSink; }
        /** 返回数据库 Sink 配置。 */
        public DbSink getDbSink() { return dbSink; }
        /** 设置数据库 Sink 配置。 */
        public void setDbSink(DbSink dbSink) { this.dbSink = dbSink; }

        /** 数据库消息日志 Sink 配置。 */
        public static class DbSink {
            private int batchSize = 100;
            private int queueCapacity = 10000;
            private long flushIntervalMillis = 500;

            /** 返回单次批量写入的最大行数。 */
            public int getBatchSize() { return batchSize; }
            /** 设置单次批量写入的最大行数。 */
            public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
            /** 返回内存队列容量。 */
            public int getQueueCapacity() { return queueCapacity; }
            /** 设置内存队列容量。 */
            public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
            /** 返回刷新间隔（毫秒）。 */
            public long getFlushIntervalMillis() { return flushIntervalMillis; }
            /** 设置刷新间隔（毫秒）。 */
            public void setFlushIntervalMillis(long flushIntervalMillis) { this.flushIntervalMillis = flushIntervalMillis; }
        }
    }
}
