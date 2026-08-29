package org.outboxpro.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.outboxpro.core.OutboxProPublisher;
import org.outboxpro.core.envelope.EventEnvelope;
import org.outboxpro.core.event.EventDefinition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 清单 T1/T2：业务表写入与 Outbox 写入必须同生共死。
 *
 * <p>关闭 Relay 与 Consumer，专注验证事务边界：
 * 事务提交时业务行与 Outbox PENDING 记录同时可见；
 * 事务回滚时两者一起消失，不留孤儿 Outbox。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, OutboxTransactionIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.relay-enabled=false",
                "outboxpro.consumer.enabled=false",
                "outboxpro.producer.poll-interval=1h"
        })
class OutboxTransactionIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库，避免其他上下文的 Relay 认领本类留下的记录。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "tx");
    }

    private static final String EVENT_TYPE = "it.order.created";

    @Autowired
    OrderTestService orderService;

    @Autowired
    JdbcTemplate jdbc;

    /** 测试专用配置：注册事件定义与模拟业务下单服务。 */
    @Configuration
    static class Config {

        @Bean
        EventDefinition<OrderCreatedPayload> orderCreatedDefinition() {
            // 事件定义中的交换机在事务测试中不会被真正投递（Relay 已关闭）
            return EventDefinition.<OrderCreatedPayload>builder()
                    .eventType(EVENT_TYPE)
                    .schemaVersion("v1")
                    .payloadType(OrderCreatedPayload.class)
                    .route("it.tx.exchange", EVENT_TYPE)
                    .build();
        }

        @Bean
        OrderTestService orderTestService(JdbcTemplate jdbc, OutboxProPublisher publisher) {
            return new OrderTestService(jdbc, publisher);
        }
    }

    /** 模拟业务服务：业务表写入与事件发布发生在同一个本地事务中。 */
    @Service
    static class OrderTestService {
        private final JdbcTemplate jdbc;
        private final OutboxProPublisher publisher;

        OrderTestService(JdbcTemplate jdbc, OutboxProPublisher publisher) {
            this.jdbc = jdbc;
            this.publisher = publisher;
        }

        /** 正常路径：业务行 + Outbox 记录一起提交。 @return 已发布的事件信封 */
        @Transactional
        public EventEnvelope<OrderCreatedPayload> placeOrder(long orderId) {
            jdbc.update("INSERT INTO business_order (id, amount) VALUES (?, ?)", orderId, 100);
            return publisher.publish(EVENT_TYPE, new OrderCreatedPayload(orderId));
        }

        /** 回滚路径：发布之后业务抛出异常，事务必须整体回滚。 */
        @Transactional
        public void placeOrderThenFail(long orderId) {
            jdbc.update("INSERT INTO business_order (id, amount) VALUES (?, ?)", orderId, 100);
            publisher.publish(EVENT_TYPE, new OrderCreatedPayload(orderId));
            throw new IllegalStateException("boom after publish");
        }
    }

    @BeforeEach
    void createBusinessTable() {
        // 幂等建表：业务表由测试自行管理，不依赖框架 DDL
        jdbc.execute("CREATE TABLE IF NOT EXISTS business_order (" +
                "id BIGINT PRIMARY KEY, amount BIGINT, " +
                "created_time DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6))");
    }

    /**
     * 清理本类产生的 Outbox 记录：测试容器数据库跨上下文共享，
     * 残留的 PENDING 记录会被其他上下文的 Relay 认领并反复发布到不存在的交换机。
     */
    @AfterEach
    void cleanOutboxRows() {
        jdbc.update("DELETE FROM outboxpro_outbox WHERE event_type = ?", EVENT_TYPE);
        jdbc.update("DELETE FROM business_order");
    }

    /** T1：事务提交后，业务行与 Outbox PENDING 记录同时可见。 */
    @Test
    void commitPersistsBusinessRowAndOutboxPending() {
        EventEnvelope<OrderCreatedPayload> envelope = orderService.placeOrder(1001L);

        Integer businessRows = jdbc.queryForObject(
                "SELECT COUNT(*) FROM business_order WHERE id = 1001", Integer.class);
        assertThat(businessRows).isEqualTo(1);

        Integer outboxRows = jdbc.queryForObject(
                "SELECT COUNT(*) FROM outboxpro_outbox WHERE event_id = ?", Integer.class, envelope.getEventId());
        assertThat(outboxRows).isEqualTo(1);

        String status = jdbc.queryForObject(
                "SELECT status FROM outboxpro_outbox WHERE event_id = ?", String.class, envelope.getEventId());
        assertThat(status).isEqualTo("PENDING");
    }

    /** T2：事务回滚后，业务行与 Outbox 记录一起消失，不留孤儿消息。 */
    @Test
    void rollbackRemovesBothBusinessRowAndOutbox() {
        long beforeOutbox = jdbc.queryForObject("SELECT COUNT(*) FROM outboxpro_outbox", Long.class);
        long beforeBusiness = jdbc.queryForObject("SELECT COUNT(*) FROM business_order", Long.class);

        assertThatThrownBy(() -> orderService.placeOrderThenFail(2002L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("boom");

        long afterOutbox = jdbc.queryForObject("SELECT COUNT(*) FROM outboxpro_outbox", Long.class);
        long afterBusiness = jdbc.queryForObject("SELECT COUNT(*) FROM business_order", Long.class);
        assertThat(afterOutbox).as("回滚后 Outbox 不应残留新记录").isEqualTo(beforeOutbox);
        assertThat(afterBusiness).as("回滚后业务行不应残留").isEqualTo(beforeBusiness);
    }
}
