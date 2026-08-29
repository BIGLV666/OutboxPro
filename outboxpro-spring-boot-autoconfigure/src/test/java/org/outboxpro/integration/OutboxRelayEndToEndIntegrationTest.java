package org.outboxpro.integration;

import org.junit.jupiter.api.Test;
import org.outboxpro.core.OutboxProPublisher;
import org.outboxpro.core.envelope.EventEnvelope;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.outboxpro.persistence.OutboxRelay;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 清单 T4：最小端到端链路。
 *
 * <p>业务事务内写 Outbox → 手动触发一次 Relay → RabbitMQ Publisher Confirm 到达后
 * 标记 SENT → 消息真实出现在绑定队列中，且携带框架 Header。</p>
 *
 * <p>调度器轮询间隔被调大，Relay 由测试手动触发，保证断言确定性。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, OutboxRelayEndToEndIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=1h",
                "outboxpro.consumer.enabled=false"
        })
class OutboxRelayEndToEndIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库，避免其他上下文的 Relay 认领本类留下的记录。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "e2e");
    }

    private static final String EVENT_TYPE = "it.order.created";
    private static final String EXCHANGE = "it.e2e.exchange";
    private static final String ROUTING_KEY = "it.order.created";
    private static final String QUEUE = "it.e2e.queue";

    @Autowired
    OutboxProPublisher publisher;

    @Autowired
    OutboxRelay relay;

    @Autowired
    TransactionTemplate transactionTemplate;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    JdbcTemplate jdbc;

    /** 测试专用配置：事件定义 + 订阅声明（拓扑由框架在启动时创建）。 */
    @Configuration
    static class Config {

        @Bean
        EventDefinition<OrderCreatedPayload> orderCreatedDefinition() {
            return EventDefinition.<OrderCreatedPayload>builder()
                    .eventType(EVENT_TYPE)
                    .schemaVersion("v1")
                    .payloadType(OrderCreatedPayload.class)
                    .route(EXCHANGE, ROUTING_KEY)
                    .build();
        }

        @Bean
        OutboxProSubscription orderSubscription() {
            return OutboxProSubscription.builder()
                    .name("it-e2e-subscription")
                    .exchange(EXCHANGE)
                    .queue(QUEUE)
                    .bindings(EventBinding.reliable(EVENT_TYPE, ROUTING_KEY, OrderCreatedPayload.class))
                    .build();
        }
    }

    /** T4：写 Outbox → Relay 认领并发布 → Confirm → SENT → 队列可见。 */
    @Test
    void relayPublishesToQueueAndMarksSentAfterConfirm() {
        EventEnvelope<OrderCreatedPayload> envelope = transactionTemplate.execute(status ->
                publisher.publish(EVENT_TYPE, new OrderCreatedPayload(3003L), Map.of("orderChannel", "integration-test")));

        assertThat(outboxStatus(envelope.getEventId())).as("发布前应为 PENDING").isEqualTo("PENDING");

        relay.relayOnce();

        Message message = rabbitTemplate.receive(QUEUE, Duration.ofSeconds(10).toMillis());
        assertThat(message).as("消息应在 Publisher Confirm 后真实到达队列").isNotNull();
        String body = new String(message.getBody());
        assertThat(body).contains(envelope.getEventId()).contains("3003");
        assertThat((String) message.getMessageProperties().getHeader("x-outboxpro-event-id")).isEqualTo(envelope.getEventId());
        assertThat((String) message.getMessageProperties().getHeader("x-outboxpro-event-type")).isEqualTo(EVENT_TYPE);

        // Confirm 到达后必须标记 SENT（允许 Relay 状态更新与队列可见之间存在毫秒级时差）
        assertThat(awaitStatus(envelope.getEventId(), "SENT")).isTrue();
        Long sentCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM outboxpro_outbox WHERE event_id = ? AND sent_time IS NOT NULL",
                Long.class, envelope.getEventId());
        assertThat(sentCount).isEqualTo(1);
    }

    /** 查询 Outbox 当前状态。 */
    private String outboxStatus(String eventId) {
        return jdbc.queryForObject("SELECT status FROM outboxpro_outbox WHERE event_id = ?", String.class, eventId);
    }

    /** 在 5 秒内等待 Outbox 到达期望状态。 */
    private boolean awaitStatus(String eventId, String expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (expected.equals(outboxStatus(eventId))) {
                return true;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return expected.equals(outboxStatus(eventId));
    }
}
