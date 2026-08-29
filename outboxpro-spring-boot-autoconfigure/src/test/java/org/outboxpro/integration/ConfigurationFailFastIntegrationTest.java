package org.outboxpro.integration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 清单 T39：非法配置在启动时 fail-fast，错误信息明确。
 *
 * <p>死信告警的恢复阈值必须小于告警阈值；违反该约束时启动必须直接失败，
 * 而不是带着矛盾的配置静默运行。使用手工启动的 SpringApplication 验证，
 * 并断言根因是配置校验异常。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
class ConfigurationFailFastIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** T39：非法告警阈值组合 → 启动即失败。 */
    @Test
    void invalidAlertConfigurationFailsFast() {
        // 手动启动的应用不走 registerIsolatedDatabase，这里按白名单字面量建库
        String serverUrl = mysql().getJdbcUrl().replaceFirst("/[^/?]+$", "/");
        try (java.sql.Connection connection = java.sql.DriverManager.getConnection(serverUrl, "root", "test");
             java.sql.Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_failfast");
        } catch (java.sql.SQLException error) {
            throw new IllegalStateException("无法创建隔离测试数据库", error);
        }

        Map<String, Object> properties = new HashMap<>();
        properties.put("spring.datasource.url",
                mysql().getJdbcUrl().replaceFirst("/[^/?]+$", "/outbox_it_failfast"));
        properties.put("spring.datasource.username", "root");
        properties.put("spring.datasource.password", "test");
        properties.put("spring.rabbitmq.host", rabbit().getHost());
        properties.put("spring.rabbitmq.port", String.valueOf(rabbit().getAmqpPort()));
        properties.put("spring.rabbitmq.username", rabbit().getAdminUsername());
        properties.put("spring.rabbitmq.password", rabbit().getAdminPassword());
        properties.put("outboxpro.producer.relay-enabled", "false");
        properties.put("outboxpro.consumer.enabled", "false");
        properties.put("outboxpro.dlq.alert.threshold", "50");
        // 非法：恢复阈值必须小于告警阈值
        properties.put("outboxpro.dlq.alert.recovery-threshold", "100");

        assertThatThrownBy(() -> new SpringApplicationBuilder(IntegrationTestApplication.class)
                .bannerMode(Banner.Mode.OFF)
                .properties(properties)
                .run())
                .isInstanceOf(BeanCreationException.class)
                .hasMessageContaining("outboxpro.dlq.alert requires threshold > recovery-threshold");
    }
}
