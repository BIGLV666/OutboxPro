package org.outboxpro.transport.rabbit;

import org.junit.jupiter.api.Test;
import org.outboxpro.spi.persistence.OutboxRecord;
import org.outboxpro.spi.transport.MessagePublisher.PublishOutcome;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link RabbitMessagePublisher} 管道化批量发布的单元测试：
 * 发送阶段异常、Confirm NACK 与 Confirm ACK 三种结果必须按序对应各记录，
 * 且非 Caching 连接工厂在构造期快速失败。
 */
class RabbitMessagePublisherTest {

    private static OutboxRecord record(long id, String routingKey) {
        return new OutboxRecord(id, "event-" + id, "demo.created", "v1", "demo",
                "demo.exchange", routingKey, "{}", null, null, null, "PENDING", 0, null, null, null);
    }

    /** stub 模板：send-error 路由在发送阶段抛异常；nack 路由的 Confirm 返回 NACK；其余 ACK。 */
    private RabbitTemplate stubTemplate() {
        RabbitTemplate template = mock(RabbitTemplate.class);
        when(template.getConnectionFactory()).thenReturn(new CachingConnectionFactory());
        doAnswer(invocation -> {
            String routingKey = invocation.getArgument(1);
            CorrelationData correlation = invocation.getArgument(4);
            if ("send-error".equals(routingKey)) {
                throw new IllegalStateException("connection closed");
            }
            correlation.getFuture().complete(new CorrelationData.Confirm(!"nack".equals(routingKey), null));
            return null;
        }).when(template).convertAndSend(anyString(), anyString(), any(), any(), any(CorrelationData.class));
        return template;
    }

    @Test
    void publishAllMapsSendErrorNackAndAckToOrderedOutcomes() {
        List<PublishOutcome> outcomes = new RabbitMessagePublisher(stubTemplate(), 1000)
                .publishAll(List.of(
                        record(1, "ok"),
                        record(2, "send-error"),
                        record(3, "nack")));

        assertThat(outcomes).hasSize(3);
        assertThat(outcomes.get(0).success()).as("Confirm ACK 应成功").isTrue();
        assertThat(outcomes.get(1).success()).as("发送异常应失败").isFalse();
        assertThat(outcomes.get(1).errorType()).isEqualTo(IllegalStateException.class.getName());
        assertThat(outcomes.get(2).success()).as("Confirm NACK 应失败").isFalse();
    }

    @Test
    void singlePublishThrowsWhenConfirmTimesOut() {
        // 不 stub convertAndSend：Confirm 永不到达，触发超时路径。
        RabbitTemplate template = mock(RabbitTemplate.class);
        when(template.getConnectionFactory()).thenReturn(new CachingConnectionFactory());
        RabbitMessagePublisher publisher = new RabbitMessagePublisher(template, 50);
        assertThatThrownBy(() -> publisher.publish(record(1, "timeout")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("confirm timed out or failed")
                .hasMessageContaining("event-1");
    }

    @Test
    void nonCachingConnectionFactoryFailsFast() {
        RabbitTemplate template = mock(RabbitTemplate.class);
        ConnectionFactory plainFactory = mock(ConnectionFactory.class);
        when(template.getConnectionFactory()).thenReturn(plainFactory);
        assertThatThrownBy(() -> new RabbitMessagePublisher(template, 1000))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CachingConnectionFactory")
                .hasMessageContaining("publisher-confirm-type");
    }

    @Test
    void cachingConnectionFactoryIsAccepted() {
        RabbitTemplate template = mock(RabbitTemplate.class);
        when(template.getConnectionFactory()).thenReturn(new CachingConnectionFactory());
        assertThatCode(() -> new RabbitMessagePublisher(template, 1000))
                .doesNotThrowAnyException();
    }
}
