package org.outboxpro.transport.rabbit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.outboxpro.core.context.EventContext;
import org.outboxpro.core.envelope.EventEnvelope;
import org.outboxpro.core.exception.NonRetryableEventException;
import org.outboxpro.core.handler.OutboxProHandler;
import org.outboxpro.core.metrics.OutboxMetrics;
import org.outboxpro.core.subscription.ConsumeMode;
import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.outboxpro.spi.observability.MessageLogSink;
import org.outboxpro.spi.observability.MessageStage;
import org.outboxpro.spi.observability.MessageStatus;
import org.outboxpro.spi.observability.MessageTraceRecord;
import org.outboxpro.spi.persistence.InboxRecord;
import org.outboxpro.spi.persistence.InboxRepository;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.slf4j.MDC;

/**
 * RabbitMQ 消费管理器，统一处理手动 ACK、Handler 路由、Inbox 幂等、重试和死信。
 * Reliable 模式只有在本地事务提交成功后才 ACK；重试消息成功写入 Retry Queue 后才 ACK 原消息。
 */
public final class RabbitConsumerManager implements AutoCloseable {
    private static final long REPUBLISH_CONFIRM_TIMEOUT_SECONDS = 10;

    private final ConnectionFactory connectionFactory;
    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;
    private final InboxRepository inboxRepository;
    private final TransactionTemplate transactionTemplate;
    private final List<OutboxProSubscription> subscriptions;
    private final Map<String, List<OutboxProHandler<?>>> handlers;
    private final MessageLogSink logSink;
    private final DeadLetterCoordinator deadLetterCoordinator;
    private final OutboxMetrics metrics;
    private final int concurrency;
    private final int prefetch;
    private final List<SimpleMessageListenerContainer> containers = new CopyOnWriteArrayList<>();

    /**
     * 创建消费者管理器，并在构造阶段校验 Handler eventType 不重复。
     *
     * @param connectionFactory RabbitMQ 连接工厂
     * @param rabbitTemplate 用于重试和死信转发的模板
     * @param objectMapper JSON 编解码器
     * @param inboxRepository Inbox 幂等仓储
     * @param transactionTemplate Reliable 消费的本地事务模板
     * @param subscriptions 订阅定义
     * @param handlers 业务 Handler 列表
     * @param logSink 生命周期日志 Sink，可为空
     * @param deadLetterCoordinator 死信协调器
     * @param metrics 指标上报门面，可为空
     * @param concurrency 每个队列的消费者并发数
     * @param prefetch RabbitMQ 单消费者预取数量
     */
    public RabbitConsumerManager(ConnectionFactory connectionFactory, RabbitTemplate rabbitTemplate,
                                 ObjectMapper objectMapper, InboxRepository inboxRepository,
                                 TransactionTemplate transactionTemplate,
                                 List<OutboxProSubscription> subscriptions,
                                 List<OutboxProHandler<?>> handlers,
                                 MessageLogSink logSink,
                                 DeadLetterCoordinator deadLetterCoordinator,
                                 OutboxMetrics metrics,
                                 int concurrency, int prefetch) {
        this.connectionFactory = connectionFactory;
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
        this.inboxRepository = inboxRepository;
        this.transactionTemplate = transactionTemplate;
        this.subscriptions = subscriptions == null ? List.of() : subscriptions;
        this.handlers = indexHandlers(handlers);
        this.logSink = logSink;
        this.deadLetterCoordinator = deadLetterCoordinator;
        this.metrics = metrics == null ? OutboxMetrics.NOOP : metrics;
        this.concurrency = concurrency;
        this.prefetch = prefetch;
    }

    /**
     * 为每一个 Subscription 创建独立的手动 ACK Listener Container。
     * 启动前先验证 Binding 与 Handler 的事件类型和载荷类型，避免消息到达后才发生配置错误。
     */
    public void start() {
        for (OutboxProSubscription subscription : subscriptions) {
            validateSubscription(subscription);

            SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
            container.setQueueNames(subscription.getQueue());
            // ACK 必须由本框架根据事务、重试和死信转发结果显式控制。
            container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
            container.setChannelTransacted(false);
            container.setConcurrentConsumers(concurrency);
            container.setPrefetchCount(prefetch);
            container.setConsumerTagStrategy(queue -> "outboxpro-" + subscription.getName() + "-" + UUID.randomUUID());
            container.setMessageListener((ChannelAwareMessageListener) (message, channel) -> handle(subscription, message, channel));
            container.start();
            containers.add(container);
        }
    }

    /**
     * 处理一条 RabbitMQ 消息。
     * 处理顺序不能调整：先完成数据库事务或完成 Retry/DLQ 的 Publisher Confirm，再 ACK 原消息。
     * 整个处理过程位于由消息 Header 恢复的 MDC 链路上下文中，处理结束（无论成败）都会清理，
     * 保证 Listener 线程不会被上一条消息的 traceId 污染。
     */
    private void handle(OutboxProSubscription subscription, Message message, Channel channel) throws Exception {
        try {
            applyTraceContext(message);
            doHandle(subscription, message, channel);
        } finally {
            clearTraceContext();
        }
    }

    private void doHandle(OutboxProSubscription subscription, Message message, Channel channel) throws Exception {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String payloadJson = new String(message.getBody(), StandardCharsets.UTF_8);
        String receivedRoutingKey = message.getMessageProperties().getReceivedRoutingKey();
        int attempt = headerInt(message, "x-outboxpro-attempt", 1);
        JsonNode root;
        String eventType;
        String eventId;

        try {
            // JSON 解析必须纳入死信流程，否则非法消息会直接从 Listener 抛出并无限 requeue。
            root = objectMapper.readTree(payloadJson);
            if (root == null || !root.isObject()) {
                throw new IllegalArgumentException("OutboxPro message root must be a JSON object");
            }
            eventType = text(root, "eventType");
            eventId = text(root, "eventId");
        } catch (Exception parseError) {
            // 缺失 eventId 时使用消息 ID 或内容哈希，确保重复投递仍能落到同一条死信台账。
            String fallbackEventId = fallbackEventId(message);
            // 事件类型不可知，标签统一记 unknown，防止外部输入污染指标基数。
            metrics.consumeDead("unknown", subscription.getConsumerName(), subscription.getQueue());
            try {
                deadLetterCoordinator.handle(subscription, null, payloadJson, fallbackEventId, null,
                        receivedRoutingKey, attempt,
                        new MalformedMessageException("Malformed OutboxPro message", parseError),
                        RabbitTopologyManager.deadExchange(subscription));
                channel.basicAck(deliveryTag, false);
            } catch (RuntimeException deadLetterError) {
                // 死信目标未可靠接收时保留原消息，避免解析失败变成静默丢失。
                channel.basicNack(deliveryTag, false, true);
            }
            return;
        }

        EventBinding binding = findBinding(subscription, eventType);

        // 当前 Queue 不订阅这个 eventType，继续重试没有意义，通过死信协调器处理。
        if (binding == null) {
            // 未绑定的事件类型可能是外部注入的任意字符串，指标标签统一记 unknown。
            metrics.consumeDead("unknown", subscription.getConsumerName(), subscription.getQueue());
            try {
                deadLetterCoordinator.handle(subscription, null, payloadJson, eventIdOrFallback(eventId, message),
                        eventType, receivedRoutingKey, attempt, null,
                        RabbitTopologyManager.deadExchange(subscription));
                channel.basicAck(deliveryTag, false);
            } catch (RuntimeException error) {
                // 死信处理失败，NACK + requeue。
                channel.basicNack(deliveryTag, false, true);
            }
            return;
        }

        String stableEventId = eventIdOrFallback(eventId, message);
        long startedNanos = System.nanoTime();
        try {
            // 订阅与 Handler 的归属关系在启动时校验过，这里兜底防御，避免错误路由到别的消费方。
            OutboxProHandler<?> handler = resolveHandler(subscription, eventType);
            if (handler == null) {
                throw new IllegalStateException(
                        "No handler bound to subscription '" + subscription.getName() + "' for eventType " + eventType);
            }
            metrics.consumeStarted(eventType, subscription.getConsumerName(), subscription.getQueue());
            if (binding.consumeMode() == ConsumeMode.RELIABLE) {
                processReliable(subscription, binding, handler, root, stableEventId, attempt);
            } else {
                processBestEffort(subscription, binding, handler, root, stableEventId, attempt, startedNanos);
            }

            // Reliable 分支到达这里代表 Handler、Inbox SUCCESS 和本地事务均已成功提交。
            // Best Effort 分支则保证失败已经被记为 IGNORED，因此两者都可以确认原消息。
            channel.basicAck(deliveryTag, false);
            metrics.consumeSucceeded(eventType, subscription.getConsumerName(), subscription.getQueue());
            log(stableEventId, eventType, subscription, MessageStatus.SUCCESS, attempt, startedNanos, null);
        } catch (RuntimeException error) {
            metrics.consumeFailed(eventType, subscription.getConsumerName(), subscription.getQueue());
            handleFailure(subscription, binding, payloadJson, channel, deliveryTag, stableEventId, eventType,
                    receivedRoutingKey, attempt, startedNanos, error);
        }
    }

    /**
     * 在同一数据库事务中执行 Handler 与 Inbox SUCCESS 更新。
     * 如果 Handler 或提交阶段失败，事务回滚且不会 ACK 原消息。
     */
    private void processReliable(OutboxProSubscription subscription, EventBinding binding, OutboxProHandler<?> handler,
                                 JsonNode root, String eventId, int attempt) {
        Boolean processed = transactionTemplate.execute(status -> {
            InboxRecord inbox = new InboxRecord(eventId, binding.eventType(), "RECEIVED", attempt - 1, Instant.now());
            // 已经成功处理的 eventId 不再次执行业务逻辑，直接交由调用方 ACK。
            if (!inboxRepository.tryStart(subscription.getConsumerName(), inbox)) {
                metrics.inboxDuplicate(binding.eventType(), subscription.getConsumerName());
                return false;
            }
            invoke(handler, binding.payloadType(), root);
            inboxRepository.markSuccess(subscription.getConsumerName(), eventId);
            return true;
        });

        // false 表示 Inbox 已经是 SUCCESS；没有必要重复执行 Handler。
        if (Boolean.FALSE.equals(processed)) {
            return;
        }
    }

    /**
     * 执行 Best Effort Handler。异常被记录为 IGNORED，避免无价值的无限重试阻塞业务队列。
     */
    private void processBestEffort(OutboxProSubscription subscription, EventBinding binding, OutboxProHandler<?> handler,
                                   JsonNode root, String eventId, int attempt, long startedNanos) {
        InboxRecord inbox = new InboxRecord(eventId, binding.eventType(), "RECEIVED", attempt - 1, Instant.now());
        if (!inboxRepository.tryStart(subscription.getConsumerName(), inbox)) {
            metrics.inboxDuplicate(binding.eventType(), subscription.getConsumerName());
            return;
        }
        try {
            invoke(handler, binding.payloadType(), root);
            inboxRepository.markSuccess(subscription.getConsumerName(), eventId);
        } catch (RuntimeException error) {
            inboxRepository.markIgnored(subscription.getConsumerName(), eventId, error.getMessage());
            log(eventId, binding.eventType(), subscription, MessageStatus.IGNORED, attempt, startedNanos, error);
        }
    }

    /**
     * 处理 Reliable 消费失败。Retry/DLQ 转发取得 Publisher Confirm 之前不能确认原消息。
     */
    private void handleFailure(OutboxProSubscription subscription, EventBinding binding, String payloadJson,
                               Channel channel, long deliveryTag, String eventId, String eventType,
                               String receivedRoutingKey, int attempt,
                               long startedNanos, RuntimeException error) throws Exception {
        boolean retryable = !(error instanceof NonRetryableEventException)
                && binding.retryPolicy().enabled()
                && attempt < binding.retryPolicy().maxAttempts();
        try {
            if (retryable) {
                // 先确认重试副本已经可靠写入 Retry Queue，再 ACK 当前 delivery。
                republishRetry(subscription, binding, payloadJson.getBytes(StandardCharsets.UTF_8), attempt);
                channel.basicAck(deliveryTag, false);
                metrics.consumeRetried(eventType, subscription.getConsumerName(), subscription.getQueue());
                log(eventId, eventType, subscription, MessageStatus.RETRYING, attempt, startedNanos, error);
            } else {
                // 不可重试或已耗尽次数时，通过死信协调器处理。
                deadLetterCoordinator.handle(subscription, binding, payloadJson, eventId, eventType, receivedRoutingKey,
                        attempt, error, RabbitTopologyManager.deadExchange(subscription));
                channel.basicAck(deliveryTag, false);
                metrics.consumeDead(eventType, subscription.getConsumerName(), subscription.getQueue());
                log(eventId, eventType, subscription, MessageStatus.DEAD, attempt, startedNanos, error);
            }
        } catch (RuntimeException republishError) {
            // 重试或死信副本未确认时，NACK + requeue，避免静默丢失原消息。
            channel.basicNack(deliveryTag, false, true);
            log(eventId, eventType, subscription, MessageStatus.FAILED, attempt, startedNanos, republishError);
        }
    }

    /** 把消息 Header 中的链路标识恢复到 MDC，Handler 日志即可携带同一 traceId。 */
    private void applyTraceContext(Message message) {
        String[] headerNames = {
                "x-outboxpro-trace-id", "x-outboxpro-correlation-id", "x-outboxpro-causation-id"};
        String[] mdcKeys = {"traceId", "correlationId", "causationId"};
        for (int i = 0; i < headerNames.length; i++) {
            Object value = message.getMessageProperties().getHeader(headerNames[i]);
            if (value != null && !String.valueOf(value).isBlank()) {
                MDC.put(mdcKeys[i], String.valueOf(value));
            }
        }
    }

    /** 清理本条消息引入的 MDC 链路键，防止线程复用造成串号。 */
    private void clearTraceContext() {
        MDC.remove("traceId");
        MDC.remove("correlationId");
        MDC.remove("causationId");
    }

    /** 返回消息中的 eventId；缺失时生成可跨重投递复用的稳定 ID。 */
    private String eventIdOrFallback(String eventId, Message message) {
        return eventId == null || eventId.isBlank() ? fallbackEventId(message) : eventId;
    }

    /** 优先使用 Rabbit messageId，否则使用消息体 SHA-256，避免 malformed 消息每次生成不同 ID。 */
    private String fallbackEventId(Message message) {
        String messageId = message.getMessageProperties().getMessageId();
        if (messageId != null && !messageId.isBlank()) {
            String stableMessageId = "rabbit-message-" + messageId;
            return stableMessageId.substring(0, Math.min(stableMessageId.length(), 100));
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(message.getBody());
            StringBuilder hex = new StringBuilder("malformed-");
            for (byte value : digest) {
                hex.append(String.format("%02x", value));
            }
            return hex.substring(0, Math.min(hex.length(), 100));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    /**
     * 校验 Subscription 中的每个 Binding 都能解析到类型匹配且归属明确的 Handler。
     *
     * <p>同一事件类型只注册了一个 Handler 时可直接使用；注册了多个 Handler
     * （多个消费方订阅同一事件）时，每个 Handler 必须声明与订阅 consumerName
     * 一致的 {@code consumerName()}，否则启动失败并给出修复指引。</p>
     */
    private void validateSubscription(OutboxProSubscription subscription) {
        for (EventBinding binding : subscription.getBindings()) {
            List<OutboxProHandler<?>> candidates = handlers.getOrDefault(binding.eventType(), List.of());
            if (candidates.isEmpty()) {
                throw new IllegalStateException("No handler registered for " + binding.eventType());
            }
            OutboxProHandler<?> handler = resolveHandler(subscription, binding.eventType());
            if (handler == null) {
                throw new IllegalStateException(
                        "Multiple handlers registered for eventType " + binding.eventType()
                                + "; override consumerName() to return '" + subscription.getConsumerName()
                                + "' on exactly one of them to bind it to subscription '" + subscription.getName() + "'");
            }
            if (!handler.payloadType().equals(binding.payloadType())) {
                throw new IllegalStateException("Payload type mismatch for " + binding.eventType());
            }
        }
    }

    /**
     * 解析订阅在指定事件类型上应使用的 Handler。
     *
     * <p>只有一个候选时直接返回；多个候选时要求其中恰有一个声明了与订阅
     * consumerName 一致的归属，返回 null 表示无法唯一确定。</p>
     */
    private OutboxProHandler<?> resolveHandler(OutboxProSubscription subscription, String eventType) {
        List<OutboxProHandler<?>> candidates = handlers.getOrDefault(eventType, List.of());
        if (candidates.size() == 1) {
            return candidates.get(0);
        }
        List<OutboxProHandler<?>> bound = candidates.stream()
                .filter(handler -> subscription.getConsumerName().equals(handler.consumerName()))
                .toList();
        return bound.size() == 1 ? bound.get(0) : null;
    }

    /** 按事件类型分组 Handler；同一事件类型的多个 Handler 必须各自声明不同的 consumerName。 */
    private Map<String, List<OutboxProHandler<?>>> indexHandlers(List<OutboxProHandler<?>> configuredHandlers) {
        Map<String, List<OutboxProHandler<?>>> result = new HashMap<>();
        if (configuredHandlers == null) {
            return result;
        }
        for (OutboxProHandler<?> handler : configuredHandlers) {
            result.computeIfAbsent(handler.eventType(), ignored -> new ArrayList<>()).add(handler);
        }
        return result;
    }

    private EventBinding findBinding(OutboxProSubscription subscription, String eventType) {
        return subscription.getBindings().stream()
                .filter(binding -> binding.eventType().equals(eventType))
                .findFirst()
                .orElse(null);
    }

    /** 将 JSON 中的 payload 反序列化为 Binding 指定类型，再调用业务 Handler。 */
    private void invoke(OutboxProHandler<?> rawHandler, Class<?> payloadType, JsonNode root) {
        try {
            Object payload = objectMapper.treeToValue(root.path("payload"), payloadType);
            @SuppressWarnings("unchecked")
            OutboxProHandler<Object> handler = (OutboxProHandler<Object>) rawHandler;
            EventEnvelope<Object> envelope = new EventEnvelope<>(
                    root.path("eventId").asText(),
                    root.path("eventType").asText(),
                    root.path("schemaVersion").asText("v1"),
                    root.path("producer").asText("unknown"),
                    Instant.parse(root.path("occurredAt").asText()),
                    text(root, "traceId"),
                    text(root, "correlationId"),
                    text(root, "causationId"),
                    payload,
                    objectMapper.convertValue(root.path("extensions"), Map.class));
            handler.handle(new EventContext<>(envelope));
        } catch (RuntimeException error) {
            // 保留 NonRetryableEventException 等框架语义，避免包装后被错误地重试。
            throw error;
        } catch (Exception error) {
            throw new RuntimeException("Handler invocation failed", error);
        }
    }

    /** 把失败消息写入指定重试队列；队列 TTL（来自绑定 RetryPolicy）到期后会通过 DLX 回流到主队列。 */
    private void republishRetry(OutboxProSubscription subscription, EventBinding binding, byte[] body, int failedAttempt) {
        // retry.N 承载"已失败 N 次"的消息；本次将等待第 failedAttempt+1 次尝试
        String queue = RabbitTopologyManager.retryQueue(subscription, binding, failedAttempt);
        publishWithConfirm(RabbitTopologyManager.retryExchange(subscription), queue, body, "retry", failedAttempt + 1);
    }

    /**
     * 发送重试副本并等待 Publisher Confirm。
     * 这里的确认是保护"先转发、后 ACK 原消息"语义的关键。
     * 当前处理线程的 MDC 中持有原消息的链路标识，转发时回填到 Header，重试日志可以继续串联同一 traceId。
     */
    private void publishWithConfirm(String exchange, String routingKey, byte[] body, String reason, int attempt) {
        CorrelationData correlation = new CorrelationData(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend(exchange, routingKey, body, message -> {
            message.getMessageProperties().setHeader("x-outboxpro-attempt", attempt);
            message.getMessageProperties().setHeader("x-outboxpro-reason", reason);
            message.getMessageProperties().setHeader("x-outboxpro-trace-id", MDC.get("traceId"));
            message.getMessageProperties().setHeader("x-outboxpro-correlation-id", MDC.get("correlationId"));
            message.getMessageProperties().setHeader("x-outboxpro-causation-id", MDC.get("causationId"));
            return message;
        }, correlation);
        try {
            if (!correlation.getFuture().get(REPUBLISH_CONFIRM_TIMEOUT_SECONDS, TimeUnit.SECONDS).isAck()) {
                throw new IllegalStateException("RabbitMQ rejected " + reason + " message");
            }
        } catch (Exception error) {
            throw new IllegalStateException("RabbitMQ confirm failed for " + reason + " message", error);
        }
    }

    private int headerInt(Message message, String name, int fallback) {
        Object value = message.getMessageProperties().getHeaders().get(name);
        try {
            return value == null ? fallback : Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private String text(JsonNode node, String name) {
        return node.hasNonNull(name) ? node.get(name).asText() : null;
    }

    /** 写日志时必须吞掉 Sink 的内部异常，防止观测系统影响业务消息。 */
    private void log(String eventId, String eventType, OutboxProSubscription subscription, MessageStatus status,
                     int attempt, long startedNanos, RuntimeException error) {
        if (logSink == null) {
            return;
        }
        try {
            logSink.append(new MessageTraceRecord(
                    eventId, null, eventType, MDC.get("traceId"), null, null, null,
                    subscription.getConsumerName(), subscription.getExchange(), subscription.getQueue(), null,
                    MessageStage.HANDLER, status, attempt,
                    (System.nanoTime() - startedNanos) / 1_000_000,
                    error == null ? null : error.getClass().getName(),
                    error == null ? null : error.getMessage(),
                    Instant.now()));
        } catch (RuntimeException ignored) {
            // 日志 Sink 是旁路能力，绝不允许它改变 ACK、重试或业务事务结果。
        }
    }

    /** 停止全部动态创建的 Listener Container。 */
    @Override
    public void close() {
        containers.forEach(SimpleMessageListenerContainer::stop);
        containers.clear();
    }
}

