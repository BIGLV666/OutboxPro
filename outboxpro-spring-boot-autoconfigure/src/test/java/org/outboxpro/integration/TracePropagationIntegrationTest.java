package org.outboxpro.integration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.outboxpro.core.OutboxProPublisher;
import org.outboxpro.core.context.EventContext;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.handler.OutboxProHandler;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.core.subscription.ConsumeMode;
import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.slf4j.MDC;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 清单 P1.4：Trace 全链路传播。
 *
 * <p>完整打通 MDC → Outbox → RabbitMQ Header → 消费端 MDC → Handler 日志：
 * 发布线程的 MDC traceId 落库并进入 Header；Handler 在自己的线程上能从 MDC 读到
 * 同一 traceId；重试转发的副本携带原 traceId；未设置 MDC 的消息不会串号。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, TracePropagationIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=200ms",
                "outboxpro.consumer.concurrency=1"
        })
class TracePropagationIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库，避免其他上下文的 Relay 认领本类留下的记录。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "trace");
    }

    private static final String EVENT_TYPE = "it.trace.created";
    private static final String EXCHANGE = "it.trace.exchange";
    private static final String QUEUE = "it.trace.queue";
    private static final String CONSUMER_NAME = "it-trace-consumer";

    /** Handler 观测到的 MDC traceId（按 eventId 记录）。 */
    static final Map<String, List<String>> OBSERVED_TRACES = new ConcurrentHashMap<>();
    /** Handler 执行次数（按 orderId）。 */
    static final Map<Long, Integer> INVOCATIONS = new ConcurrentHashMap<>();
    /** 首次执行抛 Retryable 异常的订单，用于验证重试副本携带 traceId。 */
    static final Set<Long> FAIL_ONCE = ConcurrentHashMap.newKeySet();

    @Autowired
    OutboxProPublisher publisher;

    @Autowired
    TransactionTemplate transactionTemplate;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    JdbcTemplate jdbc;

    /** 测试专用配置：事件定义、订阅与记录 MDC 的 Handler。 */
    @Configuration
    static class Config {

        @Bean
        EventDefinition<OrderCreatedPayload> traceEventDefinition() {
            return EventDefinition.<OrderCreatedPayload>builder()
                    .eventType(EVENT_TYPE)
                    .payloadType(OrderCreatedPayload.class)
                    .route(EXCHANGE, EVENT_TYPE)
                    .build();
        }

        @Bean
        OutboxProSubscription traceSubscription() {
            EventBinding binding = new EventBinding(EVENT_TYPE, EVENT_TYPE, OrderCreatedPayload.class,
                    ConsumeMode.RELIABLE, new RetryPolicy(true, 2, Duration.ofMillis(100), 1, Duration.ofMillis(500)));
            return OutboxProSubscription.builder()
                    .name("it-trace")
                    .exchange(EXCHANGE)
                    .queue(QUEUE)
                    .consumerName(CONSUMER_NAME)
                    .bindings(binding)
                    .build();
        }

        @Bean
        OutboxProHandler<OrderCreatedPayload> traceHandler() {
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
                    long orderId = context.getPayload().orderId();
                    INVOCATIONS.merge(orderId, 1, Integer::sum);
                    OBSERVED_TRACES.computeIfAbsent(context.getEventId(), ignored -> new CopyOnWriteArrayList<>())
                            .add(MDC.get("traceId"));
                    if (FAIL_ONCE.remove(orderId)) {
                        throw new IllegalStateException("trace drill failure");
                    }
                }
            };
        }
    }

    @BeforeEach
    void resetState() {
        OBSERVED_TRACES.clear();
        INVOCATIONS.clear();
        FAIL_ONCE.clear();
    }

    @AfterEach
    void cleanTables() {
        jdbc.update("DELETE FROM outboxpro_inbox WHERE consumer_name = ?", CONSUMER_NAME);
        jdbc.update("DELETE FROM outboxpro_outbox WHERE event_type = ?", EVENT_TYPE);
    }

    /** 在发布线程上设置 MDC 后发布事件（模拟业务请求链路）。 */
    private String publishWithTrace(long orderId, String traceId) {
        var envelope = transactionTemplate.execute(status -> {
            MDC.put("traceId", traceId);
            try {
                return publisher.publish(EVENT_TYPE, new OrderCreatedPayload(orderId));
            } finally {
                MDC.clear();
            }
        });
        return envelope.getEventId();
    }

    /** P1.4-主链路：MDC → Outbox 表 → RabbitMQ Header → 消费端 Handler 的 MDC。 */
    @Test
    void traceIdFlowsFromProducerMdcToConsumerMdc() {
        String traceId = "trace-abc-123";
        String eventId = publishWithTrace(54001L, traceId);

        // 链路第 1 环：Outbox 表记录 traceId
        String storedTrace = jdbc.queryForObject(
                "SELECT trace_id FROM outboxpro_outbox WHERE event_id = ?", String.class, eventId);
        assertThat(storedTrace).isEqualTo(traceId);

        // 链路第 2、3 环合并验证：消费活跃时不能手动 receive（会与 Listener 抢消息），
        // Handler 线程的 MDC 只能来自消息 Header，因此观测到 traceId 即证明 Header 传播生效。
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<String> observed = OBSERVED_TRACES.get(eventId);
            assertThat(observed).isNotEmpty();
            assertThat(observed.get(0)).isEqualTo(traceId);
        });
    }

    /** P1.4-重试链路：失败重试的转发副本携带原 traceId，重投递后 Handler 仍能读到。 */
    @Test
    void traceIdSurvivesRetryRepublish() {
        FAIL_ONCE.add(54002L);
        String traceId = "trace-retry-456";
        String eventId = publishWithTrace(54002L, traceId);

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(INVOCATIONS.get(54002L)).isEqualTo(2));

        List<String> observed = OBSERVED_TRACES.get(eventId);
        assertThat(observed).hasSize(2);
        assertThat(observed.get(0)).as("第一次消费的 MDC traceId").isEqualTo(traceId);
        assertThat(observed.get(1)).as("重试副本经 Header 回填后，MDC traceId 不变").isEqualTo(traceId);
    }

    /** P1.4-隔离：发布线程未设置 MDC 时，消费端 Handler 读到的 traceId 为 null，不残留上一条消息。 */
    @Test
    void messageWithoutTraceDoesNotInheritPreviousTrace() {
        String tracedEventId = publishWithTrace(54003L, "trace-before-cleanup");
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(OBSERVED_TRACES.get(tracedEventId)).isNotEmpty());

        // 正常发布（发布线程无 MDC）
        var envelope = publisher.publish(EVENT_TYPE, new OrderCreatedPayload(54004L));
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(OBSERVED_TRACES.get(envelope.getEventId())).isNotEmpty());

        assertThat(OBSERVED_TRACES.get(envelope.getEventId()).get(0))
                .as("无 MDC 的消息不得继承之前消息的 traceId（Listener 线程清理生效）")
                .isNull();
    }
}
