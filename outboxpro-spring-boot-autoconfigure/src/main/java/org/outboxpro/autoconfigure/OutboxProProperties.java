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
