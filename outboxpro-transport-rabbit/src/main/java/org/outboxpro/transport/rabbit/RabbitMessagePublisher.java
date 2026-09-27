package org.outboxpro.transport.rabbit;

import org.outboxpro.spi.persistence.OutboxRecord;
import org.outboxpro.spi.transport.MessagePublisher;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * RabbitMQ 发布器，等待 Publisher Confirm 后才向 Relay 返回成功。
 * Confirm 超时、Broker NACK 或发送异常都会转换为失败结果，由 Relay 负责安排下一次投递。
 *
 * <p>批量发布采用管道化：先写出整批消息，再统一等待各条 Confirm，
 * 把 N 次串行往返压缩为一次写出加批量等待。单条失败不影响同批其他记录的判定。</p>
 */
public final class RabbitMessagePublisher implements MessagePublisher {
    private final RabbitTemplate rabbitTemplate;
    private final long confirmTimeoutMillis;

    /**
     * 创建 RabbitMQ 发布器并开启 correlated Publisher Confirm。
     *
     * <p>Confirm 是本实现可靠性语义的根基：连接工厂必须支持 Confirm 回调。
     * 非 CachingConnectionFactory 时框架无法确保 Confirm 已开启，选择快速失败，
     * 避免所有发布在运行期静默超时进入重试直至 DEAD。</p>
     *
     * @param rabbitTemplate Spring RabbitMQ 模板
     * @param confirmTimeoutMillis Publisher Confirm 最大等待时间
     * @throws IllegalStateException 连接工厂不支持开启 Publisher Confirm 时抛出
     */
    public RabbitMessagePublisher(RabbitTemplate rabbitTemplate, long confirmTimeoutMillis) {
        this.rabbitTemplate = rabbitTemplate;
        this.confirmTimeoutMillis = confirmTimeoutMillis;
        if (rabbitTemplate.getConnectionFactory() instanceof CachingConnectionFactory caching) {
            caching.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
            caching.setPublisherReturns(true);
        } else {
            Object actualFactory = rabbitTemplate.getConnectionFactory();
            throw new IllegalStateException("OutboxPro requires a CachingConnectionFactory to enable correlated "
                    + "Publisher Confirm, but got " + (actualFactory == null ? "null" : actualFactory.getClass().getName())
                    + ". Keep Spring Boot's default spring.rabbitmq connection factory, or configure "
                    + "spring.rabbitmq.publisher-confirm-type=correlated on a CachingConnectionFactory.");
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

        awaitConfirm(correlation, record.eventId());
    }

    /**
     * 管道化批量发布：先整批写出（共享信道的写出由 Spring AMQP 串行化），
     * 再逐条等待各自 Confirm。Confirm 由 Broker 异步并发到达，等待阶段总体耗时
     * 接近最慢一条而不是所有条之和；发送阶段就失败的记录直接生成失败结果。
     */
    @Override
    public List<PublishOutcome> publishAll(List<OutboxRecord> records) {
        List<PendingConfirm> pending = new ArrayList<>(records.size());
        for (OutboxRecord record : records) {
            CorrelationData correlation = new CorrelationData(record.eventId());
            RuntimeException sendError = null;
            try {
                rabbitTemplate.convertAndSend(record.exchangeName(), record.routingKey(), record.payloadJson(),
                        message -> {
                            message.getMessageProperties().setContentType("application/json");
                            message.getMessageProperties().setHeader("x-outboxpro-event-id", record.eventId());
                            message.getMessageProperties().setHeader("x-outboxpro-event-type", record.eventType());
                            message.getMessageProperties().setHeader("x-outboxpro-trace-id", record.traceId());
                            message.getMessageProperties().setHeader("x-outboxpro-correlation-id", record.correlationId());
                            message.getMessageProperties().setHeader("x-outboxpro-causation-id", record.causationId());
                            return message;
                        }, correlation);
            } catch (RuntimeException error) {
                // 发送阶段失败（连接断开、转换异常等）的记录无需等待 Confirm。
                sendError = error;
            }
            pending.add(new PendingConfirm(record, sendError == null ? correlation : null, sendError));
        }

        List<PublishOutcome> outcomes = new ArrayList<>(pending.size());
        for (PendingConfirm item : pending) {
            if (item.sendError != null) {
                outcomes.add(PublishOutcome.failed(item.sendError));
                continue;
            }
            try {
                awaitConfirm(item.correlation, item.record.eventId());
                outcomes.add(PublishOutcome.ok());
            } catch (RuntimeException error) {
                outcomes.add(PublishOutcome.failed(error));
            }
        }
        return outcomes;
    }

    /** 等待单条 Publisher Confirm；超时、NACK 或中断都转换为带 eventId 的运行时异常。 */
    private void awaitConfirm(CorrelationData correlation, String eventId) {
        try {
            // 在未收到 Confirm 前，绝不能把对应 Outbox 记录改为 SENT。
            if (!correlation.getFuture().get(confirmTimeoutMillis, TimeUnit.MILLISECONDS).isAck()) {
                throw new IllegalStateException("RabbitMQ publisher confirm was rejected for event " + eventId);
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "RabbitMQ publisher confirm wait was interrupted for event " + eventId, error);
        } catch (Exception error) {
            throw new IllegalStateException(
                    "RabbitMQ publisher confirm timed out or failed for event " + eventId, error);
        }
    }

    /** 批量发布过程中一条记录的等待状态：记录、Confirm 句柄或发送阶段异常。 */
    private record PendingConfirm(OutboxRecord record, CorrelationData correlation, RuntimeException sendError) { }
}
