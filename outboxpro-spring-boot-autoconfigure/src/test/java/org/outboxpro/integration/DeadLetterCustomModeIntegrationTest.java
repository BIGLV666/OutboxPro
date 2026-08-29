package org.outboxpro.integration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.outboxpro.core.OutboxProPublisher;
import org.outboxpro.core.context.EventContext;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.handler.OutboxProHandler;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.core.subscription.ConsumeMode;
import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.outboxpro.spi.deadletter.DeadLetterContext;
import org.outboxpro.spi.deadletter.DeadLetterHandlingMode;
import org.outboxpro.spi.deadletter.DeadLetterHandlingResult;
import org.outboxpro.spi.deadletter.DeadLetterStrategy;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 清单 T35：handling-mode=CUSTOM —— 框架不发布 RabbitMQ DLQ，死信由用户策略完全接管。
 *
 * <p>自定义策略返回 ACCEPTED 即视为可靠接收；台账记录仍会写入（ledger 开启时），
 * 但框架不会把死信副本发布到 RabbitMQ DLQ 队列。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, DeadLetterCustomModeIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=200ms",
                "outboxpro.consumer.concurrency=1",
                "outboxpro.dlq.handling-mode=CUSTOM"
        })
class DeadLetterCustomModeIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库，避免其他上下文的 Relay 认领本类留下的记录。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "custom");
    }

    private static final String EVENT_TYPE = "it.custom.created";
    private static final String QUEUE = "it.custom.queue";
    private static final String CONSUMER_NAME = "it-custom-consumer";

    static final Set<Long> ALWAYS_FAIL = ConcurrentHashMap.newKeySet();
    static final AtomicInteger STRATEGY_CALLS = new AtomicInteger();

    @Autowired
    OutboxProPublisher publisher;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    JdbcTemplate jdbc;

    /** 测试专用配置：CUSTOM 模式死信策略与普通订阅。 */
    @Configuration
    static class Config {

        @Bean
        EventDefinition<OrderCreatedPayload> customEventDefinition() {
            return EventDefinition.<OrderCreatedPayload>builder()
                    .eventType(EVENT_TYPE)
                    .payloadType(OrderCreatedPayload.class)
                    .route("it.custom.exchange", EVENT_TYPE)
                    .build();
        }

        @Bean
        OutboxProSubscription customSubscription() {
            EventBinding binding = new EventBinding(EVENT_TYPE, EVENT_TYPE, OrderCreatedPayload.class,
                    ConsumeMode.RELIABLE, new RetryPolicy(true, 2, Duration.ofMillis(100), 1, Duration.ofMillis(500)));
            return OutboxProSubscription.builder()
                    .name("it-custom")
                    .exchange("it.custom.exchange")
                    .queue(QUEUE)
                    .consumerName(CONSUMER_NAME)
                    .bindings(binding)
                    .build();
        }

        @Bean
        OutboxProHandler<OrderCreatedPayload> customHandler() {
            return new OutboxProHandler<>() {
                @Override
                public String eventType() {
                    return EVENT_TYPE;
                }

                @Override
                public Class<OrderCreatedPayload> payloadType() {
                    return OrderCreatedPayload.class;
                }

                @Override
                public void handle(EventContext<OrderCreatedPayload> context) {
                    if (ALWAYS_FAIL.contains(context.getPayload().orderId())) {
                        throw new IllegalStateException("custom mode drill failure");
                    }
                }
            };
        }

        /** 用户完全接管的死信策略：记录调用并返回 ACCEPTED。 */
        @Bean
        DeadLetterStrategy acceptingStrategy() {
            return context -> {
                STRATEGY_CALLS.incrementAndGet();
                return DeadLetterHandlingResult.ACCEPTED;
            };
        }
    }

    @AfterEach
    void cleanMessageTables() {
        ALWAYS_FAIL.clear();
        jdbc.update("DELETE FROM outboxpro_inbox WHERE consumer_name = ?", CONSUMER_NAME);
        jdbc.update("DELETE FROM outboxpro_outbox WHERE event_type = 'it.custom.created'");
    }

    /** T35：CUSTOM 模式下死信交给用户策略，框架不发布 RabbitMQ DLQ。 */
    @Test
    void customModeDelegatesToUserStrategyWithoutRabbitDlq() {
        ALWAYS_FAIL.add(53001L);
        var envelope = publisher.publish(EVENT_TYPE, new OrderCreatedPayload(53001L));

        // 策略被调用且返回 ACCEPTED 后，台账记录到达 PENDING_REPLAY
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(STRATEGY_CALLS.get()).isGreaterThanOrEqualTo(1);
            String status = jdbc.queryForObject(
                    "SELECT status FROM outboxpro_dead_letter WHERE event_id = ? AND consumer_name = ?",
                    String.class, envelope.getEventId(), CONSUMER_NAME);
            assertThat(status).isEqualTo("PENDING_REPLAY");
        });

        // 框架不得发布 RabbitMQ DLQ 副本：DLQ 队列应保持为空
        //（CUSTOM 模式下拓扑仍会声明 DLQ 队列，但没有框架写入方）
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(rabbitTemplate.receive(QUEUE + ".dlq", Duration.ofSeconds(2).toMillis()))
                .as("CUSTOM 模式框架不应向 DLQ 队列发布死信副本").isNull();
    }
}
