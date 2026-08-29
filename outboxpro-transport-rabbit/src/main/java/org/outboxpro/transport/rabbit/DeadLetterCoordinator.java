package org.outboxpro.transport.rabbit;

import org.outboxpro.core.exception.NonRetryableEventException;
import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.outboxpro.spi.deadletter.*;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 死信协调器，统一处理框架模式与自定义模式的死信分派。
 *
 * <p>职责：</p>
 * <ul>
 *   <li>判定死信原因码</li>
 *   <li>构造不可变死信上下文</li>
 *   <li>可选写入死信台账与分桶计数</li>
 *   <li>根据配置执行框架 RabbitMQ DLQ 发布或调用用户策略</li>
 *   <li>只有目标确认接收才返回成功，否则抛异常触发 NACK + requeue</li>
 * </ul>
 */
public final class DeadLetterCoordinator {
    private static final long REPUBLISH_CONFIRM_TIMEOUT_SECONDS = 10;
    private static final long DISPATCH_LEASE_SECONDS = 60;

    private final DeadLetterHandlingMode handlingMode;
    private final DeadLetterRepository deadLetterRepository;
    private final DeadLetterStrategy deadLetterStrategy;
    private final DeadLetterAlertNotifier deadLetterNotifier;
    private final RabbitTemplate rabbitTemplate;
    private final boolean ledgerEnabled;

    /**
     * 创建死信协调器。
     *
     * @param handlingMode 框架或自定义模式
     * @param deadLetterRepository 死信台账仓储；台账关闭时为 {@code null}
     * @param deadLetterStrategy 自定义死信策略；模式为 FRAMEWORK 时可为 {@code null}
     * @param deadLetterNotifier 框架模式的旁路通知器；可为 {@code null}
     * @param rabbitTemplate 用于框架模式发布 RabbitMQ DLQ
     * @param ledgerEnabled 台账是否启用
     */
    public DeadLetterCoordinator(DeadLetterHandlingMode handlingMode,
                                 DeadLetterRepository deadLetterRepository,
                                 DeadLetterStrategy deadLetterStrategy,
                                 DeadLetterAlertNotifier deadLetterNotifier,
                                 RabbitTemplate rabbitTemplate,
                                 boolean ledgerEnabled) {
        this.handlingMode = handlingMode;
        this.deadLetterRepository = deadLetterRepository;
        this.deadLetterStrategy = deadLetterStrategy;
        this.deadLetterNotifier = deadLetterNotifier;
        this.rabbitTemplate = rabbitTemplate;
        this.ledgerEnabled = ledgerEnabled;
    }

    /**
     * 处理一条死信。
     *
     * <p>先构造不可变原因与上下文，可选写入台账，再根据模式执行策略。
     * 只有目标确认接收才正常返回；失败时抛异常触发调用方 NACK + requeue。</p>
     *
     * @param subscription 订阅定义
     * @param binding 事件绑定
     * @param payloadJson 原始消息 JSON 字符串
     * @param eventId 事件 ID
     * @param eventType 事件类型；畸形消息无法解析时为 {@code null}
     * @param attempt 当前消费尝试次数
     * @param error 导致死信的异常；未知事件类型时为 {@code null}
     * @param deadExchange 框架模式使用的 RabbitMQ DLQ 交换机
     */
    public void handle(OutboxProSubscription subscription, EventBinding binding, String payloadJson,
                       String eventId, String eventType, int attempt, RuntimeException error, String deadExchange) {
        // 保留旧签名兼容性；新调用方应传入 RabbitMQ 实际收到的 routing key。
        String routingKey = binding == null ? eventType : binding.routingKey();
        handle(subscription, binding, payloadJson, eventId, eventType, routingKey, attempt, error, deadExchange);
    }

    /**
     * 处理一条死信并保留原始 RabbitMQ 路由。
     *
     * @param subscription 订阅定义
     * @param binding 事件绑定；未知事件时为空
     * @param payloadJson 原始消息 JSON
     * @param eventId 事件 ID 或框架生成的稳定兜底 ID
     * @param eventType 事件类型
     * @param originalRoutingKey 原消息实际收到的路由键
     * @param attempt 当前消费尝试次数
     * @param error 导致死信的异常
     * @param deadExchange 框架模式使用的死信交换机
     */
    public void handle(OutboxProSubscription subscription, EventBinding binding, String payloadJson,
                       String eventId, String eventType, String originalRoutingKey, int attempt,
                       RuntimeException error, String deadExchange) {
        // 1. 判定不可变死信原因
        DeadLetterReasonCode code = determineReasonCode(error, binding, attempt);
        boolean retryable = isRetryable(error, binding);
        boolean retryExhausted = binding != null
                && binding.retryPolicy().enabled()
                && attempt >= binding.retryPolicy().maxAttempts();
        String exceptionType = error == null ? null : error.getClass().getName();
        String exceptionMessage = error == null ? null : truncate(error.getMessage());
        DeadLetterReason reason = new DeadLetterReason(code, retryable, retryExhausted, exceptionType, exceptionMessage);

        // 2. 规范化缺失的路由键，确保畸形消息也能进入可审计死信流程。
        String replayRoutingKey = originalRoutingKey;
        if (replayRoutingKey == null || replayRoutingKey.isBlank()) {
            replayRoutingKey = binding == null ? subscription.getQueue() : binding.routingKey();
        }
        DeadLetterContext context = new DeadLetterContext(
                eventId, eventType, subscription.getConsumerName(), subscription.getQueue(),
                subscription.getExchange(), replayRoutingKey,
                payloadJson, attempt, reason, Instant.now()
        );

        String dispatchOwner = null;
        if (ledgerEnabled && deadLetterRepository != null) {
            // 每次消费使用独立 owner，避免同一 JVM 内的并发投递相互覆盖租约。
            dispatchOwner = UUID.randomUUID().toString();
            boolean shouldDispatch = deadLetterRepository.beginDispatch(
                    context, dispatchOwner, Instant.now().plusSeconds(DISPATCH_LEASE_SECONDS));
            if (!shouldDispatch) {
                // 只有已有可靠死信副本时才返回，仓储不得把活跃 DISPATCHING 判为已处理。
                return;
            }
        }

        try {
            // 4. 根据处理模式执行不可替换的框架语义或用户完全接管的自定义策略。
            if (handlingMode == DeadLetterHandlingMode.FRAMEWORK) {
                handleFrameworkMode(context, deadExchange);
            } else {
                handleCustomMode(context);
            }

            if (dispatchOwner != null) {
                // 只有当前租约持有人能够完成状态迁移，过期持有人必须触发重投递。
                boolean marked = deadLetterRepository.markPendingReplay(
                        context.eventId(), context.consumerName(), dispatchOwner, Instant.now());
                if (!marked) {
                    throw new IllegalStateException(
                            "Dead letter dispatch lease was lost for event " + context.eventId());
                }
            }

            // 告警通知是旁路能力，失败不能改变已确认的死信分派结果。
            if (handlingMode == DeadLetterHandlingMode.FRAMEWORK && deadLetterNotifier != null) {
                try {
                    deadLetterNotifier.notify(context);
                } catch (RuntimeException ignored) {
                    // 旁路通知失败不应导致已经发布的死信被重复投递。
                }
            }
        } catch (RuntimeException dispatchError) {
            if (dispatchOwner != null) {
                try {
                    // 发布或用户策略失败时立即释放租约，让 RabbitMQ 重投递能够马上接管。
                    deadLetterRepository.releaseDispatch(
                            context.eventId(), context.consumerName(), dispatchOwner);
                } catch (RuntimeException releaseError) {
                    // 保留主异常，并附带租约释放异常供诊断；租约到期后仍可自动接管。
                    dispatchError.addSuppressed(releaseError);
                }
            }
            throw dispatchError;
        }
    }

    /**
     * 框架模式：固定发布 RabbitMQ DLQ 并等待 Publisher Confirm。
     *
     * @param context 不可变死信上下文
     * @param deadExchange 目标死信交换机
     */
    private void handleFrameworkMode(DeadLetterContext context, String deadExchange) {
        // 路由键与拓扑声明保持同一派生规则（订阅队列名），确保死信只进入所属订阅的 DLQ。
        // RabbitMQ Confirm 成功后才允许上层把台账迁移到 PENDING_REPLAY。
        publishWithConfirm(deadExchange, RabbitTopologyManager.deadRoutingKey(context.queue()),
                context.payloadJson().getBytes(StandardCharsets.UTF_8),
                context.reason().code().name(), context.attempt());
    }

    /**
     * 自定义模式：用户策略完全接管死信处理，只有返回 ACCEPTED 才视为可靠接收。
     *
     * @param context 不可变死信上下文
     */
    private void handleCustomMode(DeadLetterContext context) {
        if (deadLetterStrategy == null) {
            throw new IllegalStateException(
                    "handlingMode=CUSTOM but no DeadLetterStrategy bean found. " +
                    "Either provide a DeadLetterStrategy or switch to handlingMode=FRAMEWORK."
            );
        }

        DeadLetterHandlingResult result = deadLetterStrategy.handle(context);
        if (result != DeadLetterHandlingResult.ACCEPTED) {
            // 用户拒绝或失败，抛异常让外层 NACK + requeue。
            throw new IllegalStateException(
                    "Custom DLQ strategy rejected message " + context.eventId() + ". " +
                    "The original message will be requeued."
            );
        }
    }

    /**
     * 判定死信原因码。
     */
    private DeadLetterReasonCode determineReasonCode(RuntimeException error, EventBinding binding, int attempt) {
        if (error != null && eventTypeIsMalformed(error)) {
            // JSON 解析失败或消息结构不合法，继续重试没有意义。
            return DeadLetterReasonCode.MALFORMED_MESSAGE;
        }
        if (hasCause(error, NonRetryableEventException.class)) {
            // 业务明确声明不可重试，即使异常被调用链包装也必须保留原语义。
            return DeadLetterReasonCode.NON_RETRYABLE_EXCEPTION;
        }
        if (binding == null) {
            // 当前订阅未声明该事件类型。
            return DeadLetterReasonCode.UNKNOWN_EVENT_TYPE;
        }
        if (error == null) {
            // 没有异常且没有绑定时，按未知事件处理。
            return DeadLetterReasonCode.UNKNOWN_EVENT_TYPE;
        }
        if (binding.retryPolicy().enabled() && attempt >= binding.retryPolicy().maxAttempts()) {
            // 可重试但已耗尽次数
            return DeadLetterReasonCode.RETRY_EXHAUSTED;
        }
        // 其他 Handler 失败
        return DeadLetterReasonCode.HANDLER_FAILURE;
    }

    /**
     * 判断错误是否可重试。
     */
    private boolean isRetryable(RuntimeException error, EventBinding binding) {
        return !hasCause(error, NonRetryableEventException.class)
                && binding != null
                && binding.retryPolicy().enabled();
    }

    /** 判断异常是否代表消息格式或 Envelope 结构无效。 */
    private boolean eventTypeIsMalformed(RuntimeException error) {
        return hasCause(error, MalformedMessageException.class);
    }

    /** 沿 cause 链查找业务异常，避免包装异常改变不可重试语义。 */
    private boolean hasCause(Throwable error, Class<? extends Throwable> type) {
        Throwable current = error;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * 发送死信副本并等待 Publisher Confirm。
     */
    private void publishWithConfirm(String exchange, String routingKey, byte[] body, String reason, int attempt) {
        CorrelationData correlation = new CorrelationData(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend(exchange, routingKey, body, message -> {
            message.getMessageProperties().setHeader("x-outboxpro-attempt", attempt);
            message.getMessageProperties().setHeader("x-outboxpro-reason", reason);
            return message;
        }, correlation);
        try {
            if (!correlation.getFuture().get(REPUBLISH_CONFIRM_TIMEOUT_SECONDS, TimeUnit.SECONDS).isAck()) {
                throw new IllegalStateException("RabbitMQ rejected dead letter message");
            }
        } catch (Exception error) {
            throw new IllegalStateException("RabbitMQ confirm failed for dead letter message", error);
        }
    }

    /** 限制异常文本长度，避免异常信息撑大数据库行和日志。 */
    private String truncate(String value) {
        return value == null ? null : value.substring(0, Math.min(value.length(), 2000));
    }
}




