package org.outboxpro.transport.rabbit;

import org.junit.jupiter.api.Test;
import org.outboxpro.core.exception.NonRetryableEventException;
import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.outboxpro.spi.deadletter.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 死信协调器集成测试。
 *
 * <p>验证：</p>
 * <ul>
 *   <li>框架模式：固定发布 RabbitMQ DLQ，写入台账，调用旁路通知</li>
 *   <li>自定义模式：用户策略接管，ACCEPTED 写入台账，REJECTED 抛异常</li>
 *   <li>死信原因分类：不可重试、重试耗尽、未知事件、畸形消息、Handler 失败</li>
 *   <li>台账开关：关闭时不写入、不更新计数器</li>
 * </ul>
 */
class DeadLetterCoordinatorTest {

    @Test
    void frameworkMode_shouldPublishDLQAndWriteLedger() {
        // Arrange
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        DeadLetterRepository repository = mock(DeadLetterRepository.class);
        DeadLetterAlertNotifier notifier = mock(DeadLetterAlertNotifier.class);

        when(repository.beginDispatch(any(), anyString(), any())).thenReturn(true);
        when(repository.markPendingReplay(anyString(), anyString(), anyString(), any())).thenReturn(true);
        doAnswer(invocation -> {
            org.springframework.amqp.rabbit.connection.CorrelationData corr =
                    invocation.getArgument(4);
            corr.getFuture().complete(new org.springframework.amqp.rabbit.connection.CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbitTemplate).convertAndSend(anyString(), anyString(), any(byte[].class), any(), any());

        DeadLetterCoordinator coordinator = new DeadLetterCoordinator(
                DeadLetterHandlingMode.FRAMEWORK,
                repository, null, notifier, rabbitTemplate, true
        );

        OutboxProSubscription subscription = createSubscription();
        EventBinding binding = createBinding();
        RuntimeException error = new RuntimeException("Handler failed");

        // Act
        coordinator.handle(subscription, binding, "{\"eventId\":\"evt-1\"}", "evt-1", "TestEvent",
                5, error, "test.dlq");

        // Assert
        verify(repository).beginDispatch(argThat(ctx ->
                ctx.eventId().equals("evt-1") &&
                ctx.reason().code() == DeadLetterReasonCode.RETRY_EXHAUSTED
        ), anyString(), any());
        verify(repository).markPendingReplay(eq("evt-1"), eq("test-consumer"), anyString(), any());
        verify(notifier).notify(any());
    }

    @Test
    void customMode_accepted_shouldWriteLedger() {
        // Arrange
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        DeadLetterRepository repository = mock(DeadLetterRepository.class);
        DeadLetterStrategy strategy = mock(DeadLetterStrategy.class);

        when(repository.beginDispatch(any(), anyString(), any())).thenReturn(true);
        when(repository.markPendingReplay(anyString(), anyString(), anyString(), any())).thenReturn(true);
        when(strategy.handle(any())).thenReturn(DeadLetterHandlingResult.ACCEPTED);

        DeadLetterCoordinator coordinator = new DeadLetterCoordinator(
                DeadLetterHandlingMode.CUSTOM,
                repository, strategy, null, rabbitTemplate, true
        );

        OutboxProSubscription subscription = createSubscription();
        EventBinding binding = createBinding();

        // Act
        coordinator.handle(subscription, binding, "{\"eventId\":\"evt-2\"}", "evt-2", "TestEvent",
                3, new RuntimeException("Failed"), "test.dlq");

        // Assert
        verify(strategy).handle(any());
        verify(repository).markPendingReplay(eq("evt-2"), eq("test-consumer"), anyString(), any());
        verify(rabbitTemplate, never()).convertAndSend(anyString(), anyString(), any(byte[].class), any(), any());
    }

    @Test
    void customMode_rejected_shouldThrowException() {
        // Arrange
        DeadLetterStrategy strategy = mock(DeadLetterStrategy.class);
        when(strategy.handle(any())).thenReturn(DeadLetterHandlingResult.REJECTED);

        DeadLetterCoordinator coordinator = new DeadLetterCoordinator(
                DeadLetterHandlingMode.CUSTOM,
                null, strategy, null, mock(RabbitTemplate.class), false
        );

        OutboxProSubscription subscription = createSubscription();
        EventBinding binding = createBinding();

        // Act & Assert
        assertThrows(IllegalStateException.class, () ->
                coordinator.handle(subscription, binding, "{\"eventId\":\"evt-3\"}", "evt-3", "TestEvent",
                        2, new RuntimeException("Failed"), "test.dlq")
        );
    }

    @Test
    void ledgerDisabled_shouldNotWriteRepository() {
        // Arrange
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        doAnswer(invocation -> {
            org.springframework.amqp.rabbit.connection.CorrelationData corr = invocation.getArgument(4);
            corr.getFuture().complete(new org.springframework.amqp.rabbit.connection.CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbitTemplate).convertAndSend(anyString(), anyString(), any(byte[].class), any(), any());

        DeadLetterCoordinator coordinator = new DeadLetterCoordinator(
                DeadLetterHandlingMode.FRAMEWORK,
                null, null, null, rabbitTemplate, false
        );

        OutboxProSubscription subscription = createSubscription();
        EventBinding binding = createBinding();

        // Act
        coordinator.handle(subscription, binding, "{\"eventId\":\"evt-4\"}", "evt-4", "TestEvent",
                5, new RuntimeException("Failed"), "test.dlq");

        // Assert - 不会抛异常，正常发布到 RabbitMQ DLQ；路由键与拓扑 DLQ 绑定共用同一派生规则（订阅队列名）
        verify(rabbitTemplate).convertAndSend(eq("test.dlq"),
                eq(RabbitTopologyManager.deadRoutingKey(subscription.getQueue())), any(byte[].class), any(), any());
    }

    @Test
    void unknownEventType_shouldClassifyCorrectly() {
        // Arrange
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        DeadLetterRepository repository = mock(DeadLetterRepository.class);
        AtomicReference<DeadLetterReasonCode> capturedCode = new AtomicReference<>();

        when(repository.beginDispatch(any(), anyString(), any())).thenAnswer(invocation -> {
            DeadLetterContext ctx = invocation.getArgument(0);
            capturedCode.set(ctx.reason().code());
            return true;
        });
        when(repository.markPendingReplay(anyString(), anyString(), anyString(), any())).thenReturn(true);
        doAnswer(invocation -> {
            org.springframework.amqp.rabbit.connection.CorrelationData corr = invocation.getArgument(4);
            corr.getFuture().complete(new org.springframework.amqp.rabbit.connection.CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbitTemplate).convertAndSend(anyString(), anyString(), any(byte[].class), any(), any());

        DeadLetterCoordinator coordinator = new DeadLetterCoordinator(
                DeadLetterHandlingMode.FRAMEWORK,
                repository, null, null, rabbitTemplate, true
        );

        OutboxProSubscription subscription = createSubscription();

        // Act - binding 为 null 表示未知事件类型
        coordinator.handle(subscription, null, "{\"eventId\":\"evt-5\"}", "evt-5", "UnknownEvent",
                1, null, "test.dlq");

        // Assert
        assertEquals(DeadLetterReasonCode.UNKNOWN_EVENT_TYPE, capturedCode.get());
    }

    @Test
    void malformedMessage_shouldClassifyCorrectlyAndUseQueueFallbackRoute() {
        // Arrange
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        DeadLetterRepository repository = mock(DeadLetterRepository.class);
        AtomicReference<DeadLetterContext> capturedContext = new AtomicReference<>();
        when(repository.beginDispatch(any(), anyString(), any())).thenAnswer(invocation -> {
            capturedContext.set(invocation.getArgument(0));
            return true;
        });
        when(repository.markPendingReplay(anyString(), anyString(), anyString(), any())).thenReturn(true);
        doAnswer(invocation -> {
            org.springframework.amqp.rabbit.connection.CorrelationData corr = invocation.getArgument(4);
            corr.getFuture().complete(new org.springframework.amqp.rabbit.connection.CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbitTemplate).convertAndSend(anyString(), anyString(), any(byte[].class), any(), any());

        DeadLetterCoordinator coordinator = new DeadLetterCoordinator(
                DeadLetterHandlingMode.FRAMEWORK,
                repository, null, null, rabbitTemplate, true
        );

        // Act：模拟 JSON 解析失败且 Rabbit 未提供 received routing key。
        coordinator.handle(createSubscription(), null, "not-json", "malformed-1", null, null, 1,
                new MalformedMessageException("Malformed OutboxPro message", new IllegalArgumentException()),
                "test.dlq");

        // Assert：分类固定为 MALFORMED_MESSAGE，重放路由回退为原消费队列。
        assertEquals(DeadLetterReasonCode.MALFORMED_MESSAGE, capturedContext.get().reason().code());
        assertEquals("test.queue", capturedContext.get().originalRoutingKey());
    }

    @Test
    void nonRetryableException_shouldClassifyCorrectly() {
        // Arrange
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        DeadLetterRepository repository = mock(DeadLetterRepository.class);
        AtomicReference<DeadLetterReasonCode> capturedCode = new AtomicReference<>();

        when(repository.beginDispatch(any(), anyString(), any())).thenAnswer(invocation -> {
            DeadLetterContext ctx = invocation.getArgument(0);
            capturedCode.set(ctx.reason().code());
            return true;
        });
        when(repository.markPendingReplay(anyString(), anyString(), anyString(), any())).thenReturn(true);
        doAnswer(invocation -> {
            org.springframework.amqp.rabbit.connection.CorrelationData corr = invocation.getArgument(4);
            corr.getFuture().complete(new org.springframework.amqp.rabbit.connection.CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbitTemplate).convertAndSend(anyString(), anyString(), any(byte[].class), any(), any());

        DeadLetterCoordinator coordinator = new DeadLetterCoordinator(
                DeadLetterHandlingMode.FRAMEWORK,
                repository, null, null, rabbitTemplate, true
        );

        OutboxProSubscription subscription = createSubscription();
        EventBinding binding = createBinding();

        // Act
        coordinator.handle(subscription, binding, "{\"eventId\":\"evt-6\"}", "evt-6", "TestEvent",
                1, new NonRetryableEventException("Business validation failed"), "test.dlq");

        // Assert
        assertEquals(DeadLetterReasonCode.NON_RETRYABLE_EXCEPTION, capturedCode.get());
    }

    @Test
    void frameworkPublishFailure_shouldReleaseDispatchLease() {
        // Arrange：记录本次 owner，并模拟 RabbitMQ Publisher Confirm 拒绝。
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        DeadLetterRepository repository = mock(DeadLetterRepository.class);
        AtomicReference<String> dispatchOwner = new AtomicReference<>();
        when(repository.beginDispatch(any(), anyString(), any())).thenAnswer(invocation -> {
            dispatchOwner.set(invocation.getArgument(1));
            return true;
        });
        doAnswer(invocation -> {
            org.springframework.amqp.rabbit.connection.CorrelationData corr = invocation.getArgument(4);
            corr.getFuture().complete(new org.springframework.amqp.rabbit.connection.CorrelationData.Confirm(
                    false, "rejected"));
            return null;
        }).when(rabbitTemplate).convertAndSend(anyString(), anyString(), any(byte[].class), any(), any());
        DeadLetterCoordinator coordinator = new DeadLetterCoordinator(
                DeadLetterHandlingMode.FRAMEWORK, repository, null, null, rabbitTemplate, true);

        // Act & Assert：主流程抛错以触发 NACK，同时立即释放数据库分派租约。
        assertThrows(IllegalStateException.class, () -> coordinator.handle(
                createSubscription(), createBinding(), "{}", "evt-release", "TestEvent",
                5, new RuntimeException("failed"), "test.dlq"));
        verify(repository).releaseDispatch("evt-release", "test-consumer", dispatchOwner.get());
        verify(repository, never()).markPendingReplay(anyString(), anyString(), anyString(), any());
    }

    @Test
    void customStrategyRejected_withLedger_shouldReleaseDispatchLease() {
        // Arrange：用户策略完全接管，但明确拒绝接收当前死信。
        DeadLetterRepository repository = mock(DeadLetterRepository.class);
        DeadLetterStrategy strategy = mock(DeadLetterStrategy.class);
        AtomicReference<String> dispatchOwner = new AtomicReference<>();
        when(repository.beginDispatch(any(), anyString(), any())).thenAnswer(invocation -> {
            dispatchOwner.set(invocation.getArgument(1));
            return true;
        });
        when(strategy.handle(any())).thenReturn(DeadLetterHandlingResult.REJECTED);
        DeadLetterCoordinator coordinator = new DeadLetterCoordinator(
                DeadLetterHandlingMode.CUSTOM, repository, strategy, null, mock(RabbitTemplate.class), true);

        // Act & Assert：拒绝结果保留原有 NACK 语义，并允许下一次重投递立即取得租约。
        assertThrows(IllegalStateException.class, () -> coordinator.handle(
                createSubscription(), createBinding(), "{}", "evt-custom-release", "TestEvent",
                5, new RuntimeException("failed"), "test.dlq"));
        verify(repository).releaseDispatch("evt-custom-release", "test-consumer", dispatchOwner.get());
    }

    @Test
    void lostDispatchLease_shouldFailAndReleaseWithoutAcknowledgingSuccess() {
        // Arrange：发布已确认，但数据库条件更新表明当前 owner 已失去租约。
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        DeadLetterRepository repository = mock(DeadLetterRepository.class);
        AtomicReference<String> dispatchOwner = new AtomicReference<>();
        when(repository.beginDispatch(any(), anyString(), any())).thenAnswer(invocation -> {
            dispatchOwner.set(invocation.getArgument(1));
            return true;
        });
        when(repository.markPendingReplay(anyString(), anyString(), anyString(), any())).thenReturn(false);
        doAnswer(invocation -> {
            org.springframework.amqp.rabbit.connection.CorrelationData corr = invocation.getArgument(4);
            corr.getFuture().complete(new org.springframework.amqp.rabbit.connection.CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbitTemplate).convertAndSend(anyString(), anyString(), any(byte[].class), any(), any());
        DeadLetterCoordinator coordinator = new DeadLetterCoordinator(
                DeadLetterHandlingMode.FRAMEWORK, repository, null, null, rabbitTemplate, true);

        // Act & Assert：旧持有人不能覆盖新租约，调用必须失败并交由 RabbitMQ 重投递。
        assertThrows(IllegalStateException.class, () -> coordinator.handle(
                createSubscription(), createBinding(), "{}", "evt-lost-lease", "TestEvent",
                5, new RuntimeException("failed"), "test.dlq"));
        verify(repository).releaseDispatch("evt-lost-lease", "test-consumer", dispatchOwner.get());
    }

    private OutboxProSubscription createSubscription() {
        return OutboxProSubscription.builder()
                .name("test-subscription")
                .consumerName("test-consumer")
                .exchange("test.exchange")
                .queue("test.queue")
                .bindings(createBinding())
                .build();
    }

    private EventBinding createBinding() {
        return EventBinding.reliable("TestEvent", "test.key", String.class);
    }
}
