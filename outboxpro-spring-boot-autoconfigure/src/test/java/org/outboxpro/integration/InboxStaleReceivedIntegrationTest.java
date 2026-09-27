package org.outboxpro.integration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.outboxpro.core.context.EventContext;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.handler.OutboxProHandler;
import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Inbox RECEIVED 孤儿记录重开集成测试：
 * Best Effort 进程崩溃留下的 RECEIVED 行，超过孤儿超时后必须允许重投递重开并重新执行；
 * 未超时的 RECEIVED 行仍然必须跳过，避免并发重复执行。
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, InboxStaleReceivedIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=1h",
                "outboxpro.producer.relay-enabled=false",
                "outboxpro.consumer.inbox-received-stale-timeout=1m"
        })
class InboxStaleReceivedIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "staleinbox");
    }

    private static final String EVENT_TYPE = "it.staleinbox.created";
    private static final String EXCHANGE = "it.staleinbox.exchange";
    private static final String ROUTING_KEY = "it.staleinbox.created";
    private static final String QUEUE = "it.staleinbox.queue";
    private static final String CONSUMER_NAME = "stale-inbox-consumer";

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    JdbcTemplate jdbc;

    /** 测试专用配置：事件定义 + 订阅 + 计数 Handler。 */
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
                    .name("it-staleinbox-subscription")
                    .consumerName(CONSUMER_NAME)
                    .exchange(EXCHANGE)
                    .queue(QUEUE)
                    .bindings(EventBinding.reliable(EVENT_TYPE, ROUTING_KEY, OrderCreatedPayload.class))
                    .build();
        }

        @Bean
        CountingHandler countingHandler() {
            return new CountingHandler();
        }
    }

    /** 记录每个 eventId 的执行次数。 */
    static class CountingHandler implements OutboxProHandler<OrderCreatedPayload> {
        final Map<String, AtomicInteger> invocations = new ConcurrentHashMap<>();

        @Override public String eventType() { return EVENT_TYPE; }
        @Override public Class<OrderCreatedPayload> payloadType() { return OrderCreatedPayload.class; }
        @Override public void handle(EventContext<OrderCreatedPayload> context) {
            invocations.computeIfAbsent(context.getEnvelope().getEventId(), ignored -> new AtomicInteger())
                    .incrementAndGet();
        }

        int countOf(String eventId) {
            AtomicInteger counter = invocations.get(eventId);
            return counter == null ? 0 : counter.get();
        }
    }

    @Autowired
    CountingHandler handler;

    /** 超时的 RECEIVED 孤儿行必须被重开：Handler 重新执行，行迁移 SUCCESS 且 retry_count 累加。 */
    @Test
    void staleReceivedRowIsReopenedAndExecuted() {
        String eventId = "it-stale-reopen-" + System.nanoTime();
        insertInboxRow(eventId, "RECEIVED", Instant.now().minusSeconds(600));

        deliver(eventId, 101L);

        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "SELECT status, retry_count FROM outboxpro_inbox WHERE consumer_name = ? AND event_id = ?",
                    CONSUMER_NAME, eventId);
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).get("status")).as("孤儿 RECEIVED 重开后应到达 SUCCESS").isEqualTo("SUCCESS");
        });
        assertThat(handler.countOf(eventId)).as("孤儿记录必须重新执行业务").isEqualTo(1);
    }

    /** 未超时的 RECEIVED 行必须跳过执行（防并发重复），行保持 RECEIVED。 */
    @Test
    void freshReceivedRowIsSkippedWithoutExecution() throws InterruptedException {
        String eventId = "it-stale-fresh-" + System.nanoTime();
        insertInboxRow(eventId, "RECEIVED", Instant.now());

        deliver(eventId, 202L);

        // 等待消息确实被消费（跳过路径同样会 ACK），再断言业务未执行。
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        List<Map<String, Object>> rows;
        do {
            TimeUnit.MILLISECONDS.sleep(200);
            rows = jdbc.queryForList(
                    "SELECT status, updated_time FROM outboxpro_inbox WHERE consumer_name = ? AND event_id = ?",
                    CONSUMER_NAME, eventId);
        } while (System.nanoTime() < deadline && rows.isEmpty());
        assertThat(rows).as("Inbox 行应存在").hasSize(1);
        assertThat(rows.get(0).get("status")).as("跳过路径不得改写 RECEIVED").isEqualTo("RECEIVED");
        assertThat(handler.countOf(eventId)).as("未超时 RECEIVED 不应执行业务").isZero();
    }

    /** 直接向业务队列投递一条手工构造的 Envelope 消息。 */
    private void deliver(String eventId, long orderId) {
        String json = "{\"eventId\":\"" + eventId + "\",\"eventType\":\"" + EVENT_TYPE + "\","
                + "\"schemaVersion\":\"v1\",\"producer\":\"it\",\"occurredAt\":\"" + Instant.now() + "\","
                + "\"payload\":{\"orderId\":" + orderId + "}}";
        rabbitTemplate.send(QUEUE, new Message(json.getBytes(StandardCharsets.UTF_8), new MessageProperties()));
    }

    /** 预置一条 Inbox 行。 */
    private void insertInboxRow(String eventId, String status, Instant updatedTime) {
        jdbc.update("""
                INSERT INTO outboxpro_inbox (consumer_name, event_id, event_type, status,
                    retry_count, received_time, processed_time, updated_time, version)
                VALUES (?, ?, ?, ?, 0, ?, NULL, ?, 0)
                """, CONSUMER_NAME, eventId, EVENT_TYPE, status,
                java.sql.Timestamp.from(updatedTime), java.sql.Timestamp.from(updatedTime));
    }
}
