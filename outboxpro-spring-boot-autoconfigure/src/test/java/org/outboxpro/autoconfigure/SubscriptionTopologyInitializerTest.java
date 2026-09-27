package org.outboxpro.autoconfigure;

import org.junit.jupiter.api.Test;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.outboxpro.spi.transport.TopologyManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link SubscriptionTopologyInitializer} 的 Retry Queue 总量校验单元测试：
 * 超限启动失败并给出收缩指引；未超限正常声明且数量与绑定配置一致。
 */
class SubscriptionTopologyInitializerTest {

    /** 记录声明调用的拓扑管理器桩。 */
    private static final class RecordingTopologyManager implements TopologyManager {
        final List<OutboxProSubscription> declared = new ArrayList<>();
        @Override public void declare(OutboxProSubscription subscription) { declared.add(subscription); }
    }

    private static OutboxProSubscription subscription(String queue, int maxAttempts, boolean retryEnabled) {
        EventBinding binding = new EventBinding("it.event", "it.event", Object.class,
                org.outboxpro.core.subscription.ConsumeMode.RELIABLE,
                retryEnabled
                        ? new RetryPolicy(true, maxAttempts, Duration.ZERO, 1, Duration.ZERO)
                        : new RetryPolicy(false, 1, Duration.ZERO, 1, Duration.ZERO));
        return OutboxProSubscription.builder()
                .name("sub-" + queue)
                .exchange("it.exchange")
                .queue(queue)
                .bindings(binding)
                .build();
    }

    @Test
    void exceedingRetryQueueLimitMustFailFast() {
        // 3 条订阅 × 每绑定 9 条重试队列（maxAttempts=10）= 27，超过上限 20。
        List<OutboxProSubscription> subscriptions = List.of(
                subscription("q1", 10, true), subscription("q2", 10, true), subscription("q3", 10, true));
        SubscriptionTopologyInitializer initializer = new SubscriptionTopologyInitializer(
                new RecordingTopologyManager(), subscriptions, 20);
        assertThatThrownBy(initializer::initialize)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("27 retry queues")
                .hasMessageContaining("outboxpro.consumer.max-retry-queue-count");
    }

    @Test
    void disabledRetryAndSingleAttemptBindingsDeclareNoRetryQueues() {
        // 关闭重试与 maxAttempts=1 都不派生重试队列，即使上限为 0 也不触发校验失败。
        List<OutboxProSubscription> subscriptions = List.of(
                subscription("q1", 5, false), subscription("q2", 1, true));
        RecordingTopologyManager manager = new RecordingTopologyManager();
        SubscriptionTopologyInitializer initializer = new SubscriptionTopologyInitializer(
                manager, subscriptions, 0);
        assertThatCode(initializer::initialize).doesNotThrowAnyException();
        assertThat(manager.declared).hasSize(2);
    }

    @Test
    void withinLimitDeclaresAllSubscriptions() {
        List<OutboxProSubscription> subscriptions = List.of(
                subscription("q1", 4, true), subscription("q2", 4, true));
        RecordingTopologyManager manager = new RecordingTopologyManager();
        SubscriptionTopologyInitializer initializer = new SubscriptionTopologyInitializer(
                manager, subscriptions, 100);
        assertThatCode(initializer::initialize).doesNotThrowAnyException();
        assertThat(manager.declared).hasSize(2);
    }
}
