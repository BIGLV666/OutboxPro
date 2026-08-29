package org.outboxpro.transport.rabbit;

import org.outboxpro.spi.persistence.OutboxRecord;
import org.outboxpro.spi.transport.MessagePublisher;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.concurrent.TimeUnit;

/**
 * RabbitMQ 发布器，等待 Publisher Confirm 后才向 Relay 返回成功。
 * Confirm 超时、Broker NACK 或发送异常都会转换为 RuntimeException，由 Relay 负责安排下一次投递。
 */
public final class RabbitMessagePublisher implements MessagePublisher {
    private final RabbitTemplate rabbitTemplate;
    private final long confirmTimeoutMillis;

    /**
     * 创建 RabbitMQ 发布器并开启 correlated Publisher Confirm。
     *
     * @param rabbitTemplate Spring RabbitMQ 模板
     * @param confirmTimeoutMillis Publisher Confirm 最大等待时间
     */
    public RabbitMessagePublisher(RabbitTemplate rabbitTemplate, long confirmTimeoutMillis) {
        this.rabbitTemplate = rabbitTemplate;
        this.confirmTimeoutMillis = confirmTimeoutMillis;
        if (rabbitTemplate.getConnectionFactory() instanceof CachingConnectionFactory caching) {
            caching.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
            caching.setPublisherReturns(true);
        }
    }

    /**
     * 发布已被 Relay 认领的消息。
     * 只有 Broker Confirm ACK 才会正常返回；调用方收到异常时必须保持消息可重试。
     */
    @Override
    public void publish(OutboxRecord record) {
        CorrelationData correlation = new CorrelationData(record.eventId());
        rabbitTemplate.convertAndSend(record.exchangeName(), record.routingKey(), record.payloadJson(), message -> {
            // 头部与 Envelope 元数据重复保存，便于 RabbitMQ 侧诊断、链路关联和非 JSON 工具查看。
            message.getMessageProperties().setContentType("application/json");
            message.getMessageProperties().setHeader("x-outboxpro-event-id", record.eventId());
            message.getMessageProperties().setHeader("x-outboxpro-event-type", record.eventType());
            message.getMessageProperties().setHeader("x-outboxpro-trace-id", record.traceId());
            message.getMessageProperties().setHeader("x-outboxpro-correlation-id", record.correlationId());
            message.getMessageProperties().setHeader("x-outboxpro-causation-id", record.causationId());
            return message;
        }, correlation);

        try {
            // 在未收到 Confirm 前，绝不能把对应 Outbox 记录改为 SENT。
            if (!correlation.getFuture().get(confirmTimeoutMillis, TimeUnit.MILLISECONDS).isAck()) {
                throw new IllegalStateException("RabbitMQ publisher confirm was rejected for event " + record.eventId());
            }
        } catch (Exception error) {
            throw new IllegalStateException(
                    "RabbitMQ publisher confirm timed out or failed for event " + record.eventId(), error);
        }
    }
}
