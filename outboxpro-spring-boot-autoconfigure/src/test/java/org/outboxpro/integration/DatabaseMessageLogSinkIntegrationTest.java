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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 清单 P1.2：数据库消息日志 Sink（异步批量写入）。
 *
 * <p>验证：消费端 HANDLER 日志异步批量落库到 outboxpro_message_log；
 * traceId 从发布链路一直写进日志表；超长错误信息按列宽截断；
 * outboxpro.observability.message-log-sink=database 生效（默认仍为 slf4j）。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, DatabaseMessageLogSinkIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.poll-interval=200ms",
                "outboxpro.consumer.concurrency=1",
                "outboxpro.observability.message-log-sink=database",
                "outboxpro.observability.db-sink.batch-size=50",
                "outboxpro.observability.db-sink.queue-capacity=1000",
                "outboxpro.observability.db-sink.flush-interval-milliseconds=100"
        })
class DatabaseMessageLogSinkIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库，避免其他上下文的 Relay 认领本类留下的记录。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "msglog");
    }

    private static final String EVENT_TYPE = "it.msglog.created";
    private static final String QUEUE = "it.msglog.queue";
    private static final String CONSUMER_NAME = "it-msglog-consumer";

    static final Set<Long> FAIL_ONCE = ConcurrentHashMap.newKeySet();

    @Autowired
    OutboxProPublisher publisher;

    @Autowired
    TransactionTemplate transactionTemplate;

    @Autowired
    JdbcTemplate jdbc;

    /** 测试专用配置：事件/订阅/Handler（首次执行抛超长错误消息）。 */
    @Configuration
    static class Config {

        @Bean
        EventDefinition<OrderCreatedPayload> msglogEventDefinition() {
            return EventDefinition.<OrderCreatedPayload>builder()
                    .eventType(EVENT_TYPE)
                    .payloadType(OrderCreatedPayload.class)
                    .route("it.msglog.exchange", EVENT_TYPE)
                    .build();
        }

        @Bean
        OutboxProSubscription msglogSubscription() {
            EventBinding binding = new EventBinding(EVENT_TYPE, EVENT_TYPE, OrderCreatedPayload.class,
                    ConsumeMode.RELIABLE, new RetryPolicy(true, 2, Duration.ofMillis(100), 1, Duration.ofMillis(500)));
            return OutboxProSubscription.builder()
                    .name("it-msglog")
                    .exchange("it.msglog.exchange")
                    .queue(QUEUE)
                    .consumerName(CONSUMER_NAME)
                    .bindings(binding)
                    .build();
        }

        @Bean
        OutboxProHandler<OrderCreatedPayload> msglogHandler() {
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
                    if (FAIL_ONCE.remove(context.getPayload().orderId())) {
                        // 超长错误消息用于验证落库截断
                        throw new IllegalStateException("long error: " + "x".repeat(5000));
                    }
                }
            };
        }
    }

    @BeforeEach
    void resetState() {
        FAIL_ONCE.clear();
    }

    @AfterEach
    void cleanTables() {
        jdbc.update("DELETE FROM outboxpro_inbox WHERE consumer_name = ?", CONSUMER_NAME);
        jdbc.update("DELETE FROM outboxpro_outbox WHERE event_type = ?", EVENT_TYPE);
        jdbc.update("DELETE FROM outboxpro_message_log WHERE event_type = ?", EVENT_TYPE);
    }

    /** 在发布线程上设置 MDC 后发布事件。 */
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

    /** P1.2：消费日志异步批量落库，traceId 与执行结果完整。 */
    @Test
    void consumeLogIsPersistedToDatabase() {
        String eventId = publishWithTrace(56001L, "trace-msglog-001");

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "SELECT status, trace_id, consumer, queue_name, stage FROM outboxpro_message_log "
                            + "WHERE event_id = ? AND event_type = ?",
                    eventId, EVENT_TYPE);
            assertThat(rows).as("消费日志应异步批量落库").isNotEmpty();
            Map<String, Object> row = rows.get(0);
            assertThat(row.get("status")).isEqualTo("SUCCESS");
            assertThat(row.get("trace_id")).as("traceId 应随日志落库").isEqualTo("trace-msglog-001");
            assertThat(row.get("consumer")).isEqualTo(CONSUMER_NAME);
            assertThat(row.get("queue_name")).isEqualTo(QUEUE);
            assertThat(row.get("stage")).isEqualTo("HANDLER");
        });
    }

    /** P1.2：超长错误信息按列宽（2000）截断落库，不因超长导致批量写入失败。 */
    @Test
    void longErrorMessageIsTruncated() {
        FAIL_ONCE.add(56002L);
        var envelope = publisher.publish(EVENT_TYPE, new OrderCreatedPayload(56002L));

        // 第一次消费失败会记 RETRYING 日志（携带错误信息）
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "SELECT error_message, LENGTH(error_message) AS error_length FROM outboxpro_message_log "
                            + "WHERE event_id = ? AND status = 'RETRYING'",
                    envelope.getEventId());
            assertThat(rows).isNotEmpty();
            String errorMessage = (String) rows.get(0).get("error_message");
            assertThat(errorMessage).isNotNull();
            // MySQL 的 LENGTH() 返回 BIGINT，用 Number 接收避免类型强转失败
            Number length = (Number) rows.get(0).get("error_length");
            assertThat(length.longValue()).as("错误信息应按列宽 2000 截断").isLessThanOrEqualTo(2000);
            assertThat(errorMessage).startsWith("long error:");
        });
    }
}
