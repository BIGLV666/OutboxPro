package org.outboxpro.integration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.outboxpro.core.OutboxProPublisher;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.persistence.OutboxRelay;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.outboxpro.spi.transport.MessagePublisher;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 清单 T9：多实例 Relay 并发认领。
 *
 * <p>两个 OutboxRelay 实例（不同 owner）并发认领同一批 PENDING 记录：
 * 依赖 MySQL {@code FOR UPDATE SKIP LOCKED}，同一消息只允许被一个实例投递一次。
 * 队列最终收到 10 条互不重复的消息，数据库全部 SENT。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, OutboxRelayConcurrencyIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=1h",
                "outboxpro.consumer.enabled=false"
        })
class OutboxRelayConcurrencyIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库，避免其他上下文的 Relay 认领本类留下的记录。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "concurrency");
    }

    private static final String EVENT_TYPE = "it.order.created";
    private static final String EXCHANGE = "it.concurrent.exchange";
    private static final String QUEUE = "it.concurrent.queue";
    private static final int EVENT_COUNT = 10;

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

    /** 测试专用配置：并发用例事件定义与订阅路由。 */
    @Configuration
    static class Config {

        @Bean
        EventDefinition<OrderCreatedPayload> orderCreatedDefinition() {
            return EventDefinition.<OrderCreatedPayload>builder()
                    .eventType(EVENT_TYPE)
                    .payloadType(OrderCreatedPayload.class)
                    .route(EXCHANGE, EVENT_TYPE)
                    .build();
        }
    }

    @BeforeEach
    void declareTopology() {
        DirectExchange exchange = new DirectExchange(EXCHANGE);
        Queue queue = new Queue(QUEUE, true);
        Binding binding = BindingBuilder.bind(queue).to(exchange).with(EVENT_TYPE);
        amqpAdmin.declareExchange(exchange);
        amqpAdmin.declareQueue(queue);
        amqpAdmin.declareBinding(binding);
    }

    /** T9：两个 Relay 实例并发认领同一批消息，最终每条消息恰好投递一次。 */
    @Test
    void concurrentRelaysClaimDisjointBatches() throws Exception {
        List<String> eventIds = new ArrayList<>();
        for (int i = 1; i <= EVENT_COUNT; i++) {
            long orderId = 9000L + i;
            var envelope = transactionTemplate.execute(status -> publisher.publish(EVENT_TYPE, new OrderCreatedPayload(orderId)));
            eventIds.add(envelope.getEventId());
        }

        // 两个独立 Relay 实例 = 两个"应用实例"，各自 owner 随机生成
        OutboxRelay relayA = new OutboxRelay(outboxRepository, realMessagePublisher, relayPolicy(), 5, Duration.ofSeconds(30));
        OutboxRelay relayB = new OutboxRelay(outboxRepository, realMessagePublisher, relayPolicy(), 5, Duration.ofSeconds(30));

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        // 每个实例循环认领直到候选耗尽，batchSize=5 保证两个实例必然同时争抢同一批记录
        pool.submit(() -> {
            try {
                start.await();
                for (int round = 0; round < 10; round++) {
                    relayA.relayOnce();
                    TimeUnit.MILLISECONDS.sleep(20);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        pool.submit(() -> {
            try {
                start.await();
                for (int round = 0; round < 10; round++) {
                    relayB.relayOnce();
                    TimeUnit.MILLISECONDS.sleep(20);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        // 数据库：10 条全部 SENT，无其他状态
        Integer sentCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM outboxpro_outbox WHERE event_type = ? AND status = 'SENT'", Integer.class, EVENT_TYPE);
        assertThat(sentCount).as("10 条消息都应被恰好一个实例投递成功").isEqualTo(EVENT_COUNT);

        // 队列：正好 10 条且 eventId 互不重复
        Set<String> deliveredEventIds = new java.util.HashSet<>();
        org.springframework.amqp.core.Message message;
        while ((message = rabbitTemplate.receive(QUEUE, Duration.ofSeconds(2).toMillis())) != null) {
            deliveredEventIds.add((String) message.getMessageProperties().getHeader("x-outboxpro-event-id"));
        }
        assertThat(deliveredEventIds).hasSize(EVENT_COUNT).containsAll(eventIds);
    }

    /** 重试策略：不允许重试，任何失败立即暴露为测试失败（DEAD 断言会失败）。 */
    private org.outboxpro.core.retry.RetryPolicy relayPolicy() {
        return new org.outboxpro.core.retry.RetryPolicy(true, 1, Duration.ofMillis(100), 1, Duration.ofSeconds(1));
    }
}
