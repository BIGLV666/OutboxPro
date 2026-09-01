package org.outboxpro.integration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.outboxpro.autoconfigure.OutboxOpsEndpoint;
import org.outboxpro.core.OutboxProPublisher;
import org.outboxpro.core.context.EventContext;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.handler.OutboxProHandler;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.outboxpro.spi.deadletter.DlqReplayAuthorizer;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.outboxpro.spi.transport.MessagePublisher;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 运维查询端点集成测试：Outbox 检索、生产端 DEAD 重放与死信台账检索。
 *
 * <p>发布失败通过包装器注入：包装器在"故障窗口"内抛出异常，之后委托真实 Publisher，
 * 与 Relay 失败路径测试保持同一确定性失败注入方式。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, OpsEndpointIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=200ms",
                "outboxpro.consumer.concurrency=1",
                "outboxpro.retry.initial-delay=100ms",
                "outboxpro.retry.max-attempts=2",
                "outboxpro.dlq.replay.enabled=true",
                "outboxpro.dlq.ledger.max-replay-count=5",
                "outboxpro.dlq.alert.enabled=false",
                "outboxpro.ops.enabled=true"
        })
class OpsEndpointIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库，避免其他上下文的 Relay 认领本类留下的记录。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "ops");
    }

    private static final String EVENT_TYPE = "it.ops.created";
    private static final String EXCHANGE = "it.ops.exchange";
    private static final String QUEUE = "it.ops.queue";
    private static final String CONSUMER_NAME = "it-ops-consumer";

    /** 始终失败的订单集合（制造消费端死信）。 */
    static final Set<Long> ALWAYS_FAIL = ConcurrentHashMap.newKeySet();

    /** 故障窗口：为 true 时 Relay 的发布一律失败。 */
    static volatile boolean publishOutage = false;

    @Autowired
    OutboxProPublisher publisher;

    @Autowired
    OutboxOpsEndpoint opsEndpoint;

    @Autowired
    OutboxRepository outboxRepository;

    @Autowired
    JdbcTemplate jdbc;

    /** 测试专用配置：事件、订阅、Handler、故障注入发布器与放行授权器。 */
    @Configuration
    static class Config {

        @Bean
        EventDefinition<OrderCreatedPayload> opsEventDefinition() {
            return EventDefinition.<OrderCreatedPayload>builder()
                    .eventType(EVENT_TYPE)
                    .payloadType(OrderCreatedPayload.class)
                    .route(EXCHANGE, EVENT_TYPE)
                    .build();
        }

        @Bean
        OutboxProHandler<OrderCreatedPayload> opsHandler() {
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
                        throw new IllegalStateException("order " + context.getPayload().orderId() + " fails");
                    }
                }
            };
        }

        @Bean
        OutboxProSubscription opsSubscription() {
            var binding = new org.outboxpro.core.subscription.EventBinding(EVENT_TYPE, EVENT_TYPE,
                    OrderCreatedPayload.class, org.outboxpro.core.subscription.ConsumeMode.RELIABLE,
                    new RetryPolicy(true, 2, Duration.ofMillis(100), 1, Duration.ofMillis(500)));
            return OutboxProSubscription.builder()
                    .name("it-ops")
                    .exchange(EXCHANGE)
                    .queue(QUEUE)
                    .consumerName(CONSUMER_NAME)
                    .bindings(binding)
                    .build();
        }

        /** 故障注入发布器：故障窗口内抛异常，否则委托真实发布器。 */
        @Bean
        MessagePublisher opsMessagePublisher(RabbitTemplate rabbitTemplate, OutboxRepository repository) {
            MessagePublisher real = new org.outboxpro.transport.rabbit.RabbitMessagePublisher(rabbitTemplate, 10_000);
            return record -> {
                if (publishOutage) {
                    throw new IllegalStateException("simulated publish outage for " + record.eventId());
                }
                real.publish(record);
            };
        }

        /** 授权器：拒绝"未授权操作员"，其余放行。 */
        @Bean
        DlqReplayAuthorizer opsAuthorizer() {
            return (scopeOrEventId, operator) -> {
                if ("hacker".equals(operator)) {
                    throw new SecurityException("operator not allowed for " + scopeOrEventId);
                }
            };
        }
    }

    @AfterEach
    void resetOutage() {
        publishOutage = false;
        ALWAYS_FAIL.clear();
    }

    /** 清理消息表，台账只增不删，断言全部使用相对值。 */
    @AfterEach
    void cleanMessageTables() {
        jdbc.update("DELETE FROM outboxpro_inbox WHERE consumer_name = ?", CONSUMER_NAME);
        jdbc.update("DELETE FROM outboxpro_outbox WHERE event_type = ?", EVENT_TYPE);
    }

    /** Outbox 检索 + 详情 + DEAD 重放全链路。 */
    @Test
    void outboxListDetailAndDeadReplay() {
        publishOutage = true;
        var envelope = publisher.publish(EVENT_TYPE, new OrderCreatedPayload(9201L));

        // 全局重试策略 max-attempts=2：第一次 RETRY_WAITING，第二次 DEAD。
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(outboxStatus(envelope.getEventId())).isEqualTo("DEAD"));

        // 列表检索：按 DEAD 状态过滤命中，视图不含 payload。
        var page = opsEndpoint.listOutbox("DEAD", EVENT_TYPE, 0, 20, "ops-alice");
        assertThat(page.items()).anySatisfy(item -> {
            assertThat(item.eventId()).isEqualTo(envelope.getEventId());
            assertThat(item.status()).isEqualTo("DEAD");
            assertThat(item.attemptCount()).isEqualTo(2);
        });
        long total = opsEndpoint.listOutbox("DEAD", EVENT_TYPE, 0, 20, "ops-alice").total();
        assertThat(total).isGreaterThanOrEqualTo(1);

        // 详情：包含完整记录（含载荷）。
        Object detail = opsEndpoint.getOutbox(envelope.getEventId(), "ops-alice");
        assertThat(detail).isInstanceOf(org.outboxpro.spi.persistence.OutboxRecord.class);
        assertThat(((org.outboxpro.spi.persistence.OutboxRecord) detail).payloadJson())
                .contains(envelope.getEventId()).contains("9201");

        // 修复故障后人工重放：状态复位为 PENDING，由 Relay 重新投递直至 SENT。
        publishOutage = false;
        var outcome = opsEndpoint.replayOutbox(envelope.getEventId(),
                new OutboxOpsEndpoint.OpsRequest("ops-alice", "rabbit outage fixed"));
        assertThat(outcome.reset()).isTrue();
        assertThat(outcome.message()).contains("PENDING");

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(outboxStatus(envelope.getEventId())).isEqualTo("SENT"));

        // 重复重放：SENT 状态不再是 DEAD，复位返回 false。
        var second = opsEndpoint.replayOutbox(envelope.getEventId(),
                new OutboxOpsEndpoint.OpsRequest("ops-alice", "idempotent replay"));
        assertThat(second.reset()).isFalse();
    }

    /** 死信台账检索：消费失败 → DLQ 台账 → 检索过滤命中。 */
    @Test
    void dlqListReturnsLedgerSummaries() {
        long orderId = 9202L;
        ALWAYS_FAIL.add(orderId);
        var envelope = publisher.publish(EVENT_TYPE, new OrderCreatedPayload(orderId));

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var rows = jdbc.queryForList(
                    "SELECT status FROM outboxpro_dead_letter WHERE event_id = ? AND consumer_name = ?",
                    envelope.getEventId(), CONSUMER_NAME);
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).get("status")).isEqualTo("PENDING_REPLAY");
        });

        var page = opsEndpoint.listDeadLetters(null, EVENT_TYPE, CONSUMER_NAME, 0, 20, "ops-alice");
        assertThat(page.items()).anySatisfy(summary -> {
            assertThat(summary.eventId()).isEqualTo(envelope.getEventId());
            assertThat(summary.status()).isEqualTo("PENDING_REPLAY");
            assertThat(summary.reasonCode()).isEqualTo("RETRY_EXHAUSTED");
        });

        // 过滤条件不匹配时检索不到该记录。
        var miss = opsEndpoint.listDeadLetters(null, "it.other.event", CONSUMER_NAME, 0, 20, "ops-alice");
        assertThat(miss.items()).noneSatisfy(summary ->
                assertThat(summary.eventId()).isEqualTo(envelope.getEventId()));
    }

    /** 未授权操作员调用检索与重放时被拒绝。 */
    @Test
    void unauthorizedOperatorIsRejected() {
        assertThatThrownBy(() -> opsEndpoint.listOutbox("DEAD", null, 0, 20, "hacker"))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> opsEndpoint.listDeadLetters(null, null, null, 0, 20, "hacker"))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> opsEndpoint.replayOutbox("some-event",
                new OutboxOpsEndpoint.OpsRequest("hacker", "unauthorized")))
                .isInstanceOf(SecurityException.class);
    }

    /** 非法状态过滤与越界分页参数。 */
    @Test
    void invalidListParametersAreRejected() {
        assertThatThrownBy(() -> opsEndpoint.listOutbox("HACKED", null, 0, 20, "ops-alice"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("status");
        // 超大页尺寸被裁剪到安全上限，不抛异常。
        var page = opsEndpoint.listOutbox(null, EVENT_TYPE, 0, 100000, "ops-alice");
        assertThat(page.items()).isNotNull();
    }

    /** 查询 Outbox 状态。 */
    private String outboxStatus(String eventId) {
        return jdbc.queryForObject("SELECT status FROM outboxpro_outbox WHERE event_id = ?", String.class, eventId);
    }
}
