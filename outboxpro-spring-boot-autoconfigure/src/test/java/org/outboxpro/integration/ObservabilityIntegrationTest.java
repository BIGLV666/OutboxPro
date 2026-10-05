package org.outboxpro.integration;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.outboxpro.autoconfigure.OutboxProHealthIndicator;
import org.outboxpro.autoconfigure.OutboxProObservabilitySnapshot;
import org.outboxpro.spi.deadletter.DeadLetterRepository;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 积压观测集成测试：只依赖 MySQL，不启动 Rabbit 消费者与 Relay。
 *
 * <p>直接构造 Outbox 与死信台账数据，验证真实 SQL 聚合结果能进入共享快照、
 * Micrometer Gauge 与健康检查，不依赖消息链路时序。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, ObservabilityIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.relay-enabled=false",
                "outboxpro.consumer.enabled=false",
                "outboxpro.metrics.snapshot-ttl=1ms"
        })
class ObservabilityIntegrationTest {

    private static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.4"));

    static {
        MYSQL.start();
    }

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
    }

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    OutboxProObservabilitySnapshot snapshot;

    @Autowired
    OutboxProHealthIndicator healthIndicator;

    @Autowired
    OutboxRepository outboxRepository;

    @Autowired
    DeadLetterRepository deadLetterRepository;

    /** 测试专用 Micrometer 注册中心；其余自动装配使用默认 JDBC 仓储。 */
    @Configuration
    static class Config {
        @Bean
        SimpleMeterRegistry simpleMeterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @Test
    void gaugesAndHealthShouldReflectBacklogFromDatabase() {
        jdbc.update("""
                INSERT INTO outboxpro_outbox (
                    event_id, event_type, schema_version, producer, exchange_name,
                    routing_key, payload_json, status, attempt_count, updated_time, version
                ) VALUES (?, ?, 'v1', 'observability-it', 'it.obs.exchange',
                          'it.obs.created', '{}', 'PENDING', 0, NOW(), 0)
                """, "obs-pending-1", "it.obs.created");
        jdbc.update("""
                INSERT INTO outboxpro_dead_letter (
                    event_id, event_type, consumer_name, queue_name,
                    original_exchange, original_routing_key, payload_json,
                    attempt_count, reason_code, reason_retryable, reason_retry_exhausted,
                    status, replay_count, version
                ) VALUES (?, ?, ?, ?, ?, ?, '{}', 3, 'RETRY_EXHAUSTED', 1, 1,
                          'PENDING_REPLAY', 0, 0)
                """, "obs-dlq-1", "it.obs.created", "observability-it", "it.obs.queue",
                "it.obs.exchange", "it.obs.created");

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(gauge("outboxpro.outbox.backlog.pending")).isGreaterThanOrEqualTo(1.0);
            assertThat(gauge("outboxpro.dlq.backlog.pending_replay")).isEqualTo(1.0);
            assertThat(meterRegistry.get("outboxpro.dlq.backlog.by_reason")
                    .tag("reason", "RETRY_EXHAUSTED").gauge().value()).isEqualTo(1.0);
            assertThat(snapshot.get().pendingReplayCount()).isEqualTo(1L);
            assertThat(healthIndicator.health().getStatus().getCode()).isEqualTo("UP");
        });

        assertThat(outboxRepository.countByStatus()).containsEntry("PENDING", 1L);
        assertThat(deadLetterRepository.pendingReplayCountByReason()).containsEntry("RETRY_EXHAUSTED", 1L);

        Integer indexRows = jdbc.queryForObject("""
                SELECT COUNT(*) FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'outboxpro_dead_letter'
                  AND INDEX_NAME = 'idx_dead_letter_status_reason'
                """, Integer.class);
        assertThat(indexRows).isPositive();
        var plan = jdbc.queryForList("""
                EXPLAIN SELECT reason_code, COUNT(*)
                FROM outboxpro_dead_letter
                WHERE status = 'PENDING_REPLAY'
                GROUP BY reason_code
                """);
        assertThat(plan).hasSize(1);
        assertThat(plan.get(0).get("key")).isEqualTo("idx_dead_letter_status_reason");

        jdbc.update("""
                INSERT INTO outboxpro_outbox (
                    event_id, event_type, schema_version, producer, exchange_name,
                    routing_key, payload_json, status, attempt_count, updated_time, version
                ) VALUES (?, ?, 'v1', 'observability-it', 'it.obs.exchange',
                          'it.obs.created', '{}', 'DEAD', 3, NOW(), 0)
                """, "obs-dead-1", "it.obs.created");

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(healthIndicator.health().getStatus().getCode()).isEqualTo("DOWN"));
    }

    private double gauge(String name) {
        try {
            return meterRegistry.get(name).gauge().value();
        } catch (RuntimeException missing) {
            return 0.0;
        }
    }
}
