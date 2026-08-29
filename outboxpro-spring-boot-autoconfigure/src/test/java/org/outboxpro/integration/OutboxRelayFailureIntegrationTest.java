package org.outboxpro.integration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.outboxpro.core.OutboxProPublisher;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.persistence.OutboxRelay;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.outboxpro.spi.transport.MessagePublisher;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
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

import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 清单 T5–T8：Relay 失败路径与租约恢复。
 *
 * <p>通过真实 RabbitMQ 容器和 MySQL 容器验证：
 * T5 发布目标缺失导致 Confirm 失败进入 RETRY_WAITING，运维修复后重试成功；
 * T6 Broker 无响应时发布失败，消息保持可重试而不误标 SENT；
 * T7 重试耗尽进入 DEAD 且不再被认领；
 * T8 认领租约过期后由其他实例恢复完成投递。</p>
 *
 * <p>重试延迟被配置为 100ms，保证测试在秒级完成。
 * 为避免测试间相互干扰，本类不依赖 Spring 上下文中的 Relay Bean，
 * 各用例手动构造 Relay 并显式控制重试策略。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, OutboxRelayFailureIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=1h",
                "outboxpro.consumer.enabled=false",
                "outboxpro.retry.initial-delay=100ms",
                "outboxpro.retry.max-attempts=3"
        })
class OutboxRelayFailureIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库，避免其他上下文的 Relay 认领本类留下的记录。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "relay");
    }

    private static final String EVENT_TYPE = "it.order.created";
    private static final String RETRY_EXCHANGE = "it.retry.missing.exchange";
    private static final String RETRY_QUEUE = "it.retry.queue";

    @Autowired
    OutboxProPublisher publisher;

    @Autowired
    OutboxRepository outboxRepository;

    @Autowired
    MessagePublisher realMessagePublisher;

    @Autowired
    TransactionTemplate transactionTemplate;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    AmqpAdmin amqpAdmin;

    @Autowired
    JdbcTemplate jdbc;

    /** 测试专用配置：事件定义路由到一个初始不存在的交换机。 */
    @Configuration
    static class Config {

        @Bean
        EventDefinition<OrderCreatedPayload> orderCreatedDefinition() {
            return EventDefinition.<OrderCreatedPayload>builder()
                    .eventType(EVENT_TYPE)
                    .payloadType(OrderCreatedPayload.class)
                    .route(RETRY_EXCHANGE, EVENT_TYPE)
                    .build();
        }
    }

    /**
     * T5：发布失败 → RETRY_WAITING → 恢复后重试成功 → SENT → 队列可见。
     *
     * <p>失败通过包装器注入：前两次发布抛异常，之后委托真实 Publisher。
     * 说明：曾尝试用"向不存在的交换机发布"制造真实 Confirm 失败，但 RabbitMQ 的
     * confirm ACK 与 channel 404 关闭存在竞态（confirm 可能先到导致标记 SENT），
     * 不是确定性的失败注入方式，因此改为显式包装。</p>
     */
    @Test
    void confirmFailureRetriesThenSucceeds() {
        declareRetryTopology();

        var envelope = transactionTemplate.execute(status -> publisher.publish(EVENT_TYPE, new OrderCreatedPayload(5001L)));

        java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
        MessagePublisher flakyPublisher = record -> {
            if (attempts.incrementAndGet() <= 2) {
                throw new IllegalStateException("simulated confirm failure for " + record.eventId());
            }
            realMessagePublisher.publish(record);
        };
        OutboxRelay relay = new OutboxRelay(outboxRepository, flakyPublisher, retryPolicy(3), 10, Duration.ofSeconds(30));

        relay.relayOnce();
        assertThat(statusOf(envelope.getEventId()))
                .as("第一次发布失败应进入 RETRY_WAITING")
                .isEqualTo("RETRY_WAITING");

        Awaitility.await().pollDelay(Duration.ofMillis(200)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            relay.relayOnce();
            assertThat(statusOf(envelope.getEventId()))
                    .as("第三次尝试由真实 Publisher 完成，应标记 SENT")
                    .isEqualTo("SENT");
        });

        assertThat(rabbitTemplate.receive(RETRY_QUEUE, Duration.ofSeconds(5).toMillis()))
                .as("重试成功后消息应真实到达队列").isNotNull();
    }

    /** T6：Broker 无响应 → 发布异常 → 不标 SENT，进入重试且错误信息落库。 */
    @Test
    void brokerOutageLeavesMessageRetryable() throws Exception {
        // 模拟"黑洞 Broker"：接受 TCP 连接但不回应任何 AMQP 数据。
        // 客户端握手/Confirm 等待都会失败，覆盖 RabbitMessagePublisher 的失败转换路径。
        try (ServerSocket blackHole = new ServerSocket(0)) {
            Thread acceptor = new Thread(() -> {
                while (!blackHole.isClosed()) {
                    try (Socket ignored = blackHole.accept()) {
                        TimeUnit.SECONDS.sleep(10);
                    } catch (Exception failure) {
                        return;
                    }
                }
            });
            acceptor.setDaemon(true);
            acceptor.start();

            CachingConnectionFactory deadFactory = new CachingConnectionFactory("localhost", blackHole.getLocalPort());
            // 快速失败：握手超时 500ms，避免用例等待客户端默认超时
            deadFactory.getRabbitConnectionFactory().setHandshakeTimeout(500);
            deadFactory.getRabbitConnectionFactory().setConnectionTimeout(500);
            RabbitTemplate deadTemplate = new RabbitTemplate(deadFactory);
            MessagePublisher timingOutPublisher =
                    new org.outboxpro.transport.rabbit.RabbitMessagePublisher(deadTemplate, 800);

            var envelope = transactionTemplate.execute(status -> publisher.publish(EVENT_TYPE, new OrderCreatedPayload(6002L)));
            OutboxRelay relay = new OutboxRelay(outboxRepository, timingOutPublisher,
                    retryPolicy(3), 10, Duration.ofSeconds(30));

            relay.relayOnce();
            deadFactory.destroy();

            assertThat(statusOf(envelope.getEventId()))
                    .as("发布失败后必须保持可重试状态，而不是 SENT")
                    .isEqualTo("RETRY_WAITING");
            String errorMessage = jdbc.queryForObject(
                    "SELECT last_error_message FROM outboxpro_outbox WHERE event_id = ?", String.class, envelope.getEventId());
            assertThat(errorMessage).isNotBlank();
        }
    }

    /** T7：重试耗尽 → DEAD，记录错误信息，且不再被认领投递。 */
    @Test
    void retryExhaustionMarksDead() {
        MessagePublisher failingPublisher = record -> {
            throw new IllegalStateException("simulated rabbit outage for " + record.eventId());
        };
        var envelope = transactionTemplate.execute(status -> publisher.publish(EVENT_TYPE, new OrderCreatedPayload(7003L)));

        // maxAttempts=2：第一次进入 RETRY_WAITING，第二次直接 DEAD
        OutboxRelay relay = new OutboxRelay(outboxRepository, failingPublisher,
                retryPolicy(2), 10, Duration.ofSeconds(30));

        relay.relayOnce();
        assertThat(statusOf(envelope.getEventId())).isEqualTo("RETRY_WAITING");

        Awaitility.await().pollDelay(Duration.ofMillis(200)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            relay.relayOnce();
            assertThat(statusOf(envelope.getEventId())).isEqualTo("DEAD");
        });

        Map<String, Object> row = jdbc.queryForMap(
                "SELECT attempt_count, last_error_message FROM outboxpro_outbox WHERE event_id = ?",
                envelope.getEventId());
        assertThat((Integer) row.get("attempt_count")).isEqualTo(2);
        assertThat((String) row.get("last_error_message")).contains("simulated rabbit outage");

        relay.relayOnce();
        assertThat(statusOf(envelope.getEventId())).as("DEAD 记录不应再被认领").isEqualTo("DEAD");
    }

    /** T8：认领租约过期 → recoverExpiredClaims 恢复为 PENDING → 其他实例完成投递。 */
    @Test
    void expiredClaimIsRecoveredByAnotherRelay() {
        // 声明拓扑，确保恢复实例能真实投递成功（幂等声明，若 T5 已建则无副作用）
        declareRetryTopology();

        var envelope = transactionTemplate.execute(status -> publisher.publish(EVENT_TYPE, new OrderCreatedPayload(8004L)));

        // 模拟实例 A：认领后立即"宕机"（不再做任何状态更新）
        outboxRepository.recoverExpiredClaims(Instant.now());
        outboxRepository.claimBatch("crashed-instance-a", 10,
                Instant.now(), Instant.now().plus(Duration.ofMillis(200)));

        // 租约 200ms 过期后，实例 B 接管并完成投递
        Awaitility.await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            relay().relayOnce();
            assertThat(statusOf(envelope.getEventId())).isEqualTo("SENT");
        });
    }

    /** 幂等声明重试拓扑。 */
    private void declareRetryTopology() {
        DirectExchange exchange = new DirectExchange(RETRY_EXCHANGE);
        Queue queue = new Queue(RETRY_QUEUE, true);
        Binding binding = BindingBuilder.bind(queue).to(exchange).with(EVENT_TYPE);
        amqpAdmin.declareExchange(exchange);
        amqpAdmin.declareQueue(queue);
        amqpAdmin.declareBinding(binding);
    }

    /** 构造使用真实 RabbitMQ Publisher 的 Relay。 @param maxAttempts 最大尝试次数 */
    private OutboxRelay relay() {
        return new OutboxRelay(outboxRepository, realMessagePublisher, retryPolicy(3), 10, Duration.ofSeconds(30));
    }

    /** 平坦重试策略：每次延迟 100ms，便于快速验证状态流转。 */
    private RetryPolicy retryPolicy(int maxAttempts) {
        return new RetryPolicy(true, maxAttempts, Duration.ofMillis(100), 1, Duration.ofSeconds(1));
    }

    /** 查询 Outbox 状态。 */
    private String statusOf(String eventId) {
        return jdbc.queryForObject("SELECT status FROM outboxpro_outbox WHERE event_id = ?", String.class, eventId);
    }
}
