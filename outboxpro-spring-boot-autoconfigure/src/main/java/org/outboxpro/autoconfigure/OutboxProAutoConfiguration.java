package org.outboxpro.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.outboxpro.core.OutboxProPublisher;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.event.EventRegistry;
import org.outboxpro.core.handler.OutboxProHandler;
import org.outboxpro.core.metrics.OutboxMetrics;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.outboxpro.observability.DatabaseMessageLogSink;
import org.outboxpro.observability.Slf4jMessageLogSink;
import org.outboxpro.persistence.OutboxRelay;
import org.outboxpro.persistence.mysql.JdbcInboxRepository;
import org.outboxpro.persistence.mysql.JdbcOutboxRepository;
import org.outboxpro.persistence.mysql.JdbcDeadLetterRepository;
import org.outboxpro.persistence.mysql.TransactionalOutboxPublisher;
import org.outboxpro.spi.deadletter.DeadLetterAlertNotifier;
import org.outboxpro.spi.deadletter.DeadLetterHandlingMode;
import org.outboxpro.spi.deadletter.DeadLetterRepository;
import org.outboxpro.spi.deadletter.DeadLetterStrategy;
import org.outboxpro.spi.deadletter.DlqReplayAuthorizer;
import org.outboxpro.spi.observability.MessageLogSink;
import org.outboxpro.spi.persistence.InboxRepository;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.outboxpro.spi.serialization.EventSerializer;
import org.outboxpro.spi.transport.MessagePublisher;
import org.outboxpro.spi.transport.TopologyManager;
import org.outboxpro.transport.rabbit.DeadLetterCoordinator;
import org.outboxpro.transport.rabbit.RabbitConsumerManager;
import org.outboxpro.transport.rabbit.RabbitMessagePublisher;
import org.outboxpro.transport.rabbit.RabbitTopologyManager;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;

import java.util.UUID;

/**
 * OutboxPro V1 自动装配入口，默认组合 MySQL JDBC、RabbitMQ、Jackson、Relay 和 Consumer。
 * 所有基础设施 Bean 均通过 {@code @ConditionalOnMissingBean} 允许业务应用替换默认实现。
 */
@AutoConfiguration
// 本配置的 @ConditionalOnBean 依赖 DataSource、JdbcTemplate、RabbitTemplate 先完成注册；
// 若不显式声明顺序，自动配置按 FQN 字母序排序，org.outboxpro 会先于 org.springframework 求值，
// 导致整个 Publisher/Relay/Repository 链被条件跳过。
@AutoConfigureAfter({DataSourceAutoConfiguration.class, JdbcTemplateAutoConfiguration.class, RabbitAutoConfiguration.class})
@EnableScheduling
@EnableConfigurationProperties(OutboxProProperties.class)
@ConditionalOnProperty(prefix = "outboxpro", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxProAutoConfiguration {

    /** 启动时校验全局配置范围与约束（含强制 Inbox 幂等），非法配置直接 fail-fast。 */
    @Bean
    @ConditionalOnMissingBean
    OutboxProConfigurationValidator outboxProConfigurationValidator(OutboxProProperties properties) {
        return new OutboxProConfigurationValidator(properties);
    }

    /** 启动时校验死信台账、重放和告警配置之间的约束。 */
    @Bean
    @ConditionalOnMissingBean
    DeadLetterConfigurationValidator outboxProDeadLetterConfigurationValidator(OutboxProProperties properties) {
        return new DeadLetterConfigurationValidator(properties);
    }

    /** 注册事件定义容器，生产端 Publisher 仅允许发布已经注册的事件类型。 */
    @Bean
    @ConditionalOnMissingBean
    EventRegistry outboxProEventRegistry() {
        return new EventRegistry();
    }

    /** 启动时收集所有 EventDefinition Bean，并在重复 eventType 时快速失败。 */
    @Bean
    @ConditionalOnMissingBean
    EventRegistryInitializer outboxProEventRegistryInitializer(
            EventRegistry registry, ObjectProvider<EventDefinition<?>> definitions) {
        return new EventRegistryInitializer(registry, definitions.orderedStream().toList());
    }

    /**
     * 注解式装配：扫描 @OutboxHandler Bean，生成事件定义与队列订阅。
     * 没有任何注解式 Handler 时为空操作；业务方可通过注册同名 Bean 完全接管。
     */
    @Bean
    @ConditionalOnMissingBean
    AnnotationDrivenOutboxRegistrar outboxProAnnotationDrivenRegistrar(
            EventRegistry registry, ObjectProvider<OutboxProHandler<?>> handlers,
            ConfigurableListableBeanFactory beanFactory) {
        return new AnnotationDrivenOutboxRegistrar(registry, handlers.orderedStream().toList(), beanFactory);
    }

    /** 提供默认 Jackson 序列化器；用户可替换为 Avro、Protobuf 或受控 JSON 实现。 */
    @Bean
    @ConditionalOnMissingBean
    EventSerializer outboxProEventSerializer(ObjectMapper objectMapper) {
        return new JacksonEventSerializer(objectMapper);
    }

    /**
     * 指标上报门面。
     * 类路径存在 Micrometer 且容器中有 MeterRegistry 时使用 Micrometer 实现，
     * 否则降级为 no-op，业务方也可注册自己的 OutboxMetrics Bean 完全接管。
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnClass(name = "io.micrometer.core.instrument.MeterRegistry")
    OutboxMetrics outboxProMetrics(ObjectProvider<io.micrometer.core.instrument.MeterRegistry> meterRegistry) {
        io.micrometer.core.instrument.MeterRegistry registry = meterRegistry.getIfAvailable();
        return registry == null ? OutboxMetrics.NOOP : new MicrometerOutboxMetrics(registry);
    }    /** 将配置属性转换为运行时重试策略。 */
    @Bean
    @ConditionalOnMissingBean
    RetryPolicy outboxProRetryPolicy(OutboxProProperties properties) {
        OutboxProProperties.Retry retry = properties.getRetry();
        return new RetryPolicy(retry.isEnabled(), retry.getMaxAttempts(), retry.getInitialDelay(),
                retry.getMultiplier(), retry.getMaxDelay());
    }

    /**
     * 默认初始化 MySQL 表结构。
     * 生产环境如使用 Flyway/Liquibase，应显式设置 outboxpro.schema-initialize=false。
     */
    @Bean(initMethod = "initialize")
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnProperty(prefix = "outboxpro", name = "schema-initialize", havingValue = "true", matchIfMissing = true)
    OutboxProSchemaInitializer outboxProSchemaInitializer(DataSource dataSource, OutboxProProperties properties) {
        return new OutboxProSchemaInitializer(dataSource, properties);
    }

    /** 默认注入 MySQL JDBC Outbox Repository。 */
    @Bean
    @ConditionalOnClass(JdbcTemplate.class)
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnMissingBean
    OutboxRepository outboxProOutboxRepository(JdbcTemplate jdbcTemplate) {
        return new JdbcOutboxRepository(jdbcTemplate);
    }

    /** 默认注入 MySQL JDBC Inbox Repository。 */
    @Bean
    @ConditionalOnClass(JdbcTemplate.class)
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnMissingBean
    InboxRepository outboxProInboxRepository(JdbcTemplate jdbcTemplate) {
        return new JdbcInboxRepository(jdbcTemplate);
    }

    /**
     * 死信台账仓储。
     * 只有在台账启用时才装配，使用分桶计数器实现固定成本的待重放数量查询。
     */
    @Bean
    @ConditionalOnClass(JdbcTemplate.class)
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnProperty(prefix = "outboxpro.dlq.ledger", name = "enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnMissingBean
    DeadLetterRepository outboxProDeadLetterRepository(JdbcTemplate jdbcTemplate, OutboxProProperties properties) {
        return new JdbcDeadLetterRepository(jdbcTemplate, properties.getDlq().getLedger().getCounterBuckets());
    }

    /**
     * 死信协调器。
     * 根据配置模式执行框架 RabbitMQ DLQ 发布或调用用户自定义策略。
     */
    @Bean
    @ConditionalOnBean(RabbitTemplate.class)
    @ConditionalOnMissingBean
    DeadLetterCoordinator outboxProDeadLetterCoordinator(
            RabbitTemplate rabbitTemplate,
            OutboxProProperties properties,
            ObjectProvider<DeadLetterRepository> deadLetterRepository,
            ObjectProvider<DeadLetterStrategy> deadLetterStrategy,
            ObjectProvider<DeadLetterAlertNotifier> deadLetterNotifier) {
        DeadLetterQueueProperties dlq = properties.getDlq();
        return new DeadLetterCoordinator(
                dlq.getHandlingMode(),
                deadLetterRepository.getIfAvailable(),
                deadLetterStrategy.getIfAvailable(),
                deadLetterNotifier.getIfAvailable(),
                rabbitTemplate,
                dlq.getLedger().isEnabled()
        );
    }

    /**
     * 死信告警任务。
     * 只有在台账和告警都启用时才装配。
     */
    @Bean
    @ConditionalOnBean(DeadLetterRepository.class)
    @ConditionalOnProperty(prefix = "outboxpro.dlq.alert", name = "enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnMissingBean
    DeadLetterAlertTask outboxProDeadLetterAlertTask(
            DeadLetterRepository deadLetterRepository,
            ObjectProvider<DeadLetterAlertNotifier> alertNotifier,
            OutboxProProperties properties) {
        DeadLetterQueueProperties.Alert alert = properties.getDlq().getAlert();
        // 提供默认空实现
        DeadLetterAlertNotifier defaultNotifier = new DeadLetterAlertNotifier() {
            @Override
            public void onHighWatermark(long pendingCount, long threshold) {
                // 默认空实现
            }
            @Override
            public void onRecovered(long pendingCount, long recoveryThreshold) {
                // 默认空实现
            }
        };
        return new DeadLetterAlertTask(
                deadLetterRepository,
                alertNotifier.getIfAvailable(() -> defaultNotifier),
                alert.getThreshold(),
                alert.getRecoveryThreshold(),
                alert.getCooldown()
        );
    }

    /**
     * 死信重放端点。
     * 只有在台账和重放都启用时才装配。启动时强校验配置依赖。
     * 需要 Spring Boot Actuator 依赖存在。
     */
    @Bean
    @ConditionalOnClass(name = {
            "org.springframework.boot.actuate.endpoint.web.annotation.RestControllerEndpoint",
            "org.springframework.web.bind.annotation.PostMapping"
    })
    @ConditionalOnBean(DeadLetterRepository.class)
    @ConditionalOnProperty(prefix = "outboxpro.dlq.replay", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    DeadLetterReplayEndpoint outboxProDeadLetterReplayEndpoint(
            DeadLetterRepository deadLetterRepository,
            RabbitTemplate rabbitTemplate,
            ObjectMapper objectMapper,
            ObjectProvider<DlqReplayAuthorizer> authorizer,
            OutboxProProperties properties,
            Environment environment) {
        String applicationName = environment.getProperty("spring.application.name", "outboxpro-replay");
        String ownerId = applicationName + "-" + UUID.randomUUID();
        DlqReplayAuthorizer defaultDeny = (eventId, operator) -> {
            throw new SecurityException("No DlqReplayAuthorizer bean is configured");
        };
        return new DeadLetterReplayEndpoint(
                deadLetterRepository,
                authorizer.getIfAvailable(() -> defaultDeny),
                rabbitTemplate,
                objectMapper,
                properties.getDlq().getLedger().getMaxReplayCount(),
                ownerId
        );
    }

    /**
     * 为 Reliable 消费提供本地数据库事务模板。
     * Handler 业务更新和 Inbox SUCCESS 必须使用同一个事务模板。
     */
    @Bean
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnMissingBean
    TransactionTemplate outboxProTransactionTemplate(DataSource dataSource) {
        return new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    /**
     * 默认 RabbitMQ Publisher。该实现等待 correlated Publisher Confirm，失败时由 Relay 进入重试。
     *
     * <p>必须定义在 {@link #outboxProPublisher} 与 {@link #outboxProRelay} 之前：
     * 同一配置类内 {@code @ConditionalOnBean} 只能看到当前已注册的 Bean 定义，
     * 若定义在后会导致 Publisher 和 Relay 被条件跳过而永远无法装配。</p>
     */
    @Bean
    @ConditionalOnBean({ConnectionFactory.class, RabbitTemplate.class})
    @ConditionalOnMissingBean
    MessagePublisher outboxProMessagePublisher(RabbitTemplate template, OutboxProProperties properties) {
        return new RabbitMessagePublisher(template, properties.getProducer().getConfirmTimeout().toMillis());
    }

    /**
     * 创建事务型 Publisher。该 Bean 只写 Outbox，绝不会在业务事务内直接发送 RabbitMQ 消息。
     */
    @Bean
    @ConditionalOnBean({OutboxRepository.class, MessagePublisher.class})
    @ConditionalOnProperty(prefix = "outboxpro.producer", name = "enabled", havingValue = "true", matchIfMissing = true)
    OutboxProPublisher outboxProPublisher(EventRegistry registry, EventSerializer serializer,
                                          OutboxRepository repository, OutboxProProperties properties,
                                          Environment environment) {
        String producerName = environment.resolvePlaceholders(properties.getProducerName());
        return new TransactionalOutboxPublisher(registry, serializer, repository, producerName);
    }

    /** 创建 Relay；它由调度器周期触发并在事务外完成 RabbitMQ 发布。 */
    @Bean
    @ConditionalOnBean({OutboxRepository.class, MessagePublisher.class})
    @ConditionalOnProperty(prefix = "outboxpro.producer", name = "relay-enabled", havingValue = "true", matchIfMissing = true)
    OutboxRelay outboxProRelay(OutboxRepository repository, MessagePublisher publisher,
                               RetryPolicy policy, OutboxProProperties properties,
                               ObjectProvider<OutboxMetrics> metrics) {
        return new OutboxRelay(repository, publisher, policy,
                properties.getProducer().getBatchSize(), properties.getProducer().getClaimTimeout(),
                metrics.getIfAvailable(() -> OutboxMetrics.NOOP));
    }

    /** 将 OutboxRelay 挂接到 Spring 调度器。 */
    @Bean
    @ConditionalOnBean(OutboxRelay.class)
    OutboxRelayScheduler outboxProRelayScheduler(OutboxRelay relay) {
        return new OutboxRelayScheduler(relay);
    }

    /** 默认使用 SLF4J 输出消息生命周期日志；日志失败必须被隔离。 */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "outboxpro.observability", name = "message-log-sink",
            havingValue = "slf4j", matchIfMissing = true)
    MessageLogSink outboxProMessageLogSink() {
        return new Slf4jMessageLogSink();
    }

    /**
     * 数据库消息日志 Sink：异步队列 + 定时批量写入 outboxpro_message_log。
     * 队列有界（有限内存保护），批量失败整批丢弃并限流告警，不影响消息主流程。
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnProperty(prefix = "outboxpro.observability", name = "message-log-sink", havingValue = "database")
    MessageLogSink outboxProDatabaseMessageLogSink(JdbcTemplate jdbcTemplate, OutboxProProperties properties) {
        OutboxProProperties.Observability.DbSink dbSink = properties.getObservability().getDbSink();
        return new DatabaseMessageLogSink(jdbcTemplate, dbSink.getQueueCapacity(),
                dbSink.getBatchSize(), dbSink.getFlushIntervalMillis());
    }

    /**
     * RabbitAdmin 负责声明 Exchange、Queue 与 Binding。
     * 判据使用 AmqpAdmin：Spring Boot 默认的 amqpAdmin Bean 声明类型是 AmqpAdmin，
     * 按 RabbitAdmin 具体类型判断无法识别它，会导致两个管理客户端共存并使按类型注入失败。
     */
    @Bean
    @ConditionalOnBean({ConnectionFactory.class, RabbitTemplate.class})
    @ConditionalOnMissingBean(AmqpAdmin.class)
    RabbitAdmin outboxProRabbitAdmin(ConnectionFactory factory) {
        return new RabbitAdmin(factory);
    }

    /** 根据订阅和每个绑定的重试策略自动创建 RabbitMQ 业务、重试和死信拓扑。 */
    @Bean
    @ConditionalOnBean({ConnectionFactory.class, AmqpAdmin.class})
    @ConditionalOnMissingBean
    TopologyManager outboxProTopologyManager(AmqpAdmin admin) {
        return new RabbitTopologyManager(admin);
    }

    /**
     * 在启动 Consumer 前声明拓扑，避免 Listener 先启动导致 Queue 不存在或 Binding 未完成。
     * 依赖注解式注册器先完成订阅单例注册。
     */
    @Bean(initMethod = "initialize")
    @DependsOn({"outboxProEventRegistryInitializer", "outboxProAnnotationDrivenRegistrar"})
    @ConditionalOnBean(TopologyManager.class)
    SubscriptionTopologyInitializer outboxProSubscriptionTopologyInitializer(
            TopologyManager manager, ObjectProvider<OutboxProSubscription> subscriptions) {
        return new SubscriptionTopologyInitializer(manager, subscriptions.orderedStream().toList());
    }

    /**
     * 创建动态 Consumer Manager。它在 Spring 生命周期启动阶段校验 Handler 并注册手动 ACK 的监听容器。
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    @DependsOn("outboxProSubscriptionTopologyInitializer")
    @ConditionalOnBean({ConnectionFactory.class, RabbitTemplate.class, InboxRepository.class, TransactionTemplate.class, DeadLetterCoordinator.class})
    @ConditionalOnProperty(prefix = "outboxpro.consumer", name = "enabled", havingValue = "true", matchIfMissing = true)
    RabbitConsumerManager outboxProConsumerManager(
            ConnectionFactory factory, RabbitTemplate template, ObjectMapper objectMapper,
            InboxRepository inbox, TransactionTemplate transactionTemplate,
            ObjectProvider<OutboxProSubscription> subscriptions,
            ObjectProvider<OutboxProHandler<?>> handlers,
            MessageLogSink logSink, DeadLetterCoordinator deadLetterCoordinator,
            ObjectProvider<OutboxMetrics> metrics,
            OutboxProProperties properties) {
        return new RabbitConsumerManager(factory, template, objectMapper, inbox, transactionTemplate,
                subscriptions.orderedStream().toList(), handlers.orderedStream().toList(), logSink,
                deadLetterCoordinator, metrics.getIfAvailable(() -> OutboxMetrics.NOOP),
                properties.getConsumer().getConcurrency(), properties.getConsumer().getPrefetch());
    }

    /**
     * 运维查询端点：Outbox 检索、DEAD 重放与死信台账检索。
     * 默认关闭（outboxpro.ops.enabled=false），开启前应确认已配置 DlqReplayAuthorizer。
     */
    @Bean
    @ConditionalOnClass(name = {
            "org.springframework.boot.actuate.endpoint.web.annotation.RestControllerEndpoint",
            "org.springframework.web.bind.annotation.GetMapping"
    })
    @ConditionalOnBean({OutboxRepository.class, DeadLetterRepository.class})
    @ConditionalOnProperty(prefix = "outboxpro.ops", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    OutboxOpsEndpoint outboxProOpsEndpoint(
            OutboxRepository outboxRepository,
            DeadLetterRepository deadLetterRepository,
            ObjectProvider<DlqReplayAuthorizer> authorizer) {
        DlqReplayAuthorizer defaultDeny = (scope, operator) -> {
            throw new SecurityException("No DlqReplayAuthorizer bean is configured for ops scope " + scope);
        };
        return new OutboxOpsEndpoint(outboxRepository, deadLetterRepository, authorizer.getIfAvailable(() -> defaultDeny));
    }
}





