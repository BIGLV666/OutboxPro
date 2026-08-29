package org.outboxpro.integration;

import org.awaitility.Awaitility;
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
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 清单 T16–T21：RELIABLE 消费主线。
 *
 * <p>使用真实容器和框架完整装配（Relay 轮询 200ms、Consumer 开启）验证：
 * T16 成功消费写 Inbox 并 ACK；
 * T17/T18 Handler 失败后经 Retry Queue TTL 回流重试（而非立即 requeue）；
 * T19 重试耗尽进入 DLQ 和死信台账；
 * T20/T21 同一 eventId 重复投递只执行业务一次。</p>
 *
 * <p>消费并发固定为 1，保证断言确定性；绑定使用 maxAttempts=2 的快速重试策略。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, ReliableConsumeIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=200ms",
                "outboxpro.consumer.concurrency=1"
        })
class ReliableConsumeIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库，避免其他上下文的 Relay 认领本类留下的记录。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "consume");
    }

    private static final String EVENT_TYPE = "it.order.created";
    private static final String EXCHANGE = "it.consume.exchange";
    private static final String QUEUE = "it.consume.queue";
    private static final String CONSUMER_NAME = "it-consumer";
    /** 绑定重试策略的 initialDelay：retry.1 的 TTL 即该值（delayForAttempt(1)）。 */
    private static final long RETRY_TTL_MILLIS = 100;

    /** Handler 执行计数：key 为 eventId。 */
    static final ConcurrentHashMap<String, Integer> INVOCATIONS = new ConcurrentHashMap<>();
    /** 首次执行必失败的 eventId 集合，执行后自动移出，用于验证"失败一次后恢复"。 */
    static final Set<String> FAIL_ONCE = ConcurrentHashMap.newKeySet();
    /** 始终失败的 eventId 集合，用于验证重试耗尽。 */
    static final Set<String> ALWAYS_FAIL = ConcurrentHashMap.newKeySet();
    /** 记录第一次执行时间，用于断言重试经过了 TTL 延迟。 */
    static final Map<String, Long> FIRST_INVOCATION_NANOS = new ConcurrentHashMap<>();

    @Autowired
    OutboxProPublisher publisher;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    JdbcTemplate jdbc;

    /** 测试专用配置：事件定义、订阅声明与测试 Handler。 */
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

        @Bean
        OutboxProSubscription orderSubscription() {
            // maxAttempts=2：第一次失败走 Retry Queue（TTL 1s），第二次失败直接 DLQ
            EventBinding binding = new EventBinding(EVENT_TYPE, EVENT_TYPE, OrderCreatedPayload.class,
                    ConsumeMode.RELIABLE, new RetryPolicy(true, 2, Duration.ofMillis(100), 1, Duration.ofMillis(500)));
            return OutboxProSubscription.builder()
                    .name("it-consume")
                    .exchange(EXCHANGE)
                    .queue(QUEUE)
                    .consumerName(CONSUMER_NAME)
                    .bindings(binding)
                    .build();
        }

        @Bean
        OutboxProHandler<OrderCreatedPayload> testOrderHandler() {
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
                    String eventId = context.getEventId();
                    INVOCATIONS.merge(eventId, 1, Integer::sum);
                    FIRST_INVOCATION_NANOS.putIfAbsent(eventId, System.nanoTime());
                    if (ALWAYS_FAIL.contains(eventId) || FAIL_ONCE.remove(eventId)) {
                        throw new IllegalStateException("simulated handler failure for " + eventId);
                    }
                }
            };
        }
    }

    @BeforeEach
    void resetHandlerState() {
        INVOCATIONS.clear();
        FAIL_ONCE.clear();
        ALWAYS_FAIL.clear();
        FIRST_INVOCATION_NANOS.clear();
    }

    /** T16：Handler 成功 → Inbox SUCCESS → ACK → 队列清空。 */
    @Test
    void successfulHandlerWritesInboxAndAcks() {
        var envelope = publisher.publish(EVENT_TYPE, new OrderCreatedPayload(41001L));

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(INVOCATIONS).containsEntry(envelope.getEventId(), 1));

        awaitQueueDrained();

        Map<String, Object> inbox = jdbc.queryForMap(
                "SELECT status FROM outboxpro_inbox WHERE consumer_name = ? AND event_id = ?",
                CONSUMER_NAME, envelope.getEventId());
        assertThat(inbox.get("status")).isEqualTo("SUCCESS");
    }

    /** T17+T18：失败一次 → Retry Queue TTL 回流 → 第二次成功；重试间隔不小于 TTL。 */
    @Test
    void failureGoesThroughRetryQueueTtlBeforeRedelivery() {
        var envelope = publisher.publish(EVENT_TYPE, new OrderCreatedPayload(41002L));
        FAIL_ONCE.add(envelope.getEventId());

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(INVOCATIONS).containsEntry(envelope.getEventId(), 2));

        long elapsedMillis = (System.nanoTime() - FIRST_INVOCATION_NANOS.get(envelope.getEventId())) / 1_000_000;
        assertThat(elapsedMillis)
                .as("第二次执行必须经过 Retry Queue TTL 延迟，而不是立即 requeue")
                .isGreaterThanOrEqualTo(RETRY_TTL_MILLIS);

        Map<String, Object> inbox = jdbc.queryForMap(
                "SELECT status FROM outboxpro_inbox WHERE consumer_name = ? AND event_id = ?",
                CONSUMER_NAME, envelope.getEventId());
        assertThat(inbox.get("status")).isEqualTo("SUCCESS");
    }

    /** T19：重试耗尽 → DLQ + 死信台账记录，原消息 ACK。 */
    @Test
    void exhaustedRetriesLandInDlqAndLedger() {
        var envelope = publisher.publish(EVENT_TYPE, new OrderCreatedPayload(41003L));
        ALWAYS_FAIL.add(envelope.getEventId());

        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(INVOCATIONS).containsEntry(envelope.getEventId(), 2));

        // DLQ 队列应收到死信副本
        var deadMessage = rabbitTemplate.receive(QUEUE + ".dlq", Duration.ofSeconds(5).toMillis());
        assertThat(deadMessage).as("耗尽重试的消息应出现在 DLQ 队列").isNotNull();
        assertThat(new String(deadMessage.getBody(), StandardCharsets.UTF_8)).contains(envelope.getEventId());

        // 死信台账应记录原因 RETRY_EXHAUSTED
        Map<String, Object> ledger = jdbc.queryForMap(
                "SELECT reason_code, status FROM outboxpro_dead_letter WHERE event_id = ? AND consumer_name = ?",
                envelope.getEventId(), CONSUMER_NAME);
        assertThat(ledger.get("reason_code")).isEqualTo("RETRY_EXHAUSTED");

        // 业务队列应已清空（原消息被 ACK）
        awaitQueueDrained();
    }

    /** T20：同一 eventId 二次投递 → Inbox 命中，跳过业务执行，直接 ACK。 */
    @Test
    void duplicateDeliverySkipsHandler() {
        var envelope = publisher.publish(EVENT_TYPE, new OrderCreatedPayload(41004L));

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(INVOCATIONS).containsEntry(envelope.getEventId(), 1));

        // 模拟上游重复投递：把同一封Envelope 原样重新发布到主交换机
        String envelopeJson = jdbc.queryForObject(
                "SELECT payload_json FROM outboxpro_outbox WHERE event_id = ?", String.class, envelope.getEventId());
        rabbitTemplate.convertAndSend(EXCHANGE, EVENT_TYPE, envelopeJson.getBytes(StandardCharsets.UTF_8));

        awaitQueueDrained();
        awaitQueueDrained();
        assertThat(INVOCATIONS).as("重复投递不得再次执行业务").containsEntry(envelope.getEventId(), 1);
    }

    /** T21：同一 eventId 连续多次重复投递 → 业务仍只执行一次。 */
    @Test
    void repeatedDuplicateDeliveryExecutesBusinessOnlyOnce() {
        var envelope = publisher.publish(EVENT_TYPE, new OrderCreatedPayload(41005L));

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(INVOCATIONS).containsEntry(envelope.getEventId(), 1));

        String envelopeJson = jdbc.queryForObject(
                "SELECT payload_json FROM outboxpro_outbox WHERE event_id = ?", String.class, envelope.getEventId());
        for (int i = 0; i < 3; i++) {
            rabbitTemplate.convertAndSend(EXCHANGE, EVENT_TYPE, envelopeJson.getBytes(StandardCharsets.UTF_8));
        }

        awaitQueueDrained();
        awaitQueueDrained();
        awaitQueueDrained();
        assertThat(INVOCATIONS).as("多次重复投递仍只执行业务一次").containsEntry(envelope.getEventId(), 1);

        Integer duplicateSkip = jdbc.queryForObject(
                "SELECT COUNT(*) FROM outboxpro_inbox WHERE consumer_name = ? AND event_id = ?",
                Integer.class, CONSUMER_NAME, envelope.getEventId());
        assertThat(duplicateSkip).as("Inbox 唯一键保证同一 consumer+event 只有一条记录").isEqualTo(1);
    }

    /** 等待业务队列排空（连续两次探测都为空）。 */
    private void awaitQueueDrained() {
        Awaitility.await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(10)).until(() -> {
            boolean firstEmpty = rabbitTemplate.receive(QUEUE, Duration.ofSeconds(1).toMillis()) == null;
            boolean secondEmpty = rabbitTemplate.receive(QUEUE, Duration.ofSeconds(1).toMillis()) == null;
            return firstEmpty && secondEmpty;
        });
    }
}
