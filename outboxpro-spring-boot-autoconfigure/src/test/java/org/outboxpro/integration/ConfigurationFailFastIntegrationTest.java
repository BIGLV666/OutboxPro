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

    /** 手动启动的用例共用的隔离库，按需创建。 */
    private static void ensureFailFastDatabase() {
        String serverUrl = mysql().getJdbcUrl().replaceFirst("/[^/?]+$", "/");
        try (java.sql.Connection connection = java.sql.DriverManager.getConnection(serverUrl, "root", "test");
             java.sql.Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_failfast");
        } catch (java.sql.SQLException error) {
            throw new IllegalStateException("无法创建隔离测试数据库", error);
        }
    }

    /** 手动启动的用例共用的容器连接属性（数据源指向 failfast 隔离库）。 */
    private static Map<String, Object> containerProperties() {
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
        return properties;
    }

    /** T39：非法告警阈值组合 → 启动即失败。 */
    @Test
    void invalidAlertConfigurationFailsFast() {
        ensureFailFastDatabase();
        Map<String, Object> properties = containerProperties();
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

    /** 覆盖的 TransactionTemplate 使用非 REQUIRED 传播时，启动必须被契约校验器拒绝。 */
    @Test
    void overriddenTransactionTemplateWithWrongPropagationFailsFast() {
        ensureFailFastDatabase();
        assertThatThrownBy(() -> new SpringApplicationBuilder(
                IntegrationTestApplication.class, WrongPropagationTemplateConfig.class)
                .bannerMode(Banner.Mode.OFF)
                .properties(containerProperties())
                .run())
                .rootCause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PROPAGATION_REQUIRED");
    }

    /** 提供违反契约的事务模板 Bean：同名覆盖默认模板，传播行为为 REQUIRES_NEW。 */
    @org.springframework.context.annotation.Configuration
    static class WrongPropagationTemplateConfig {
        @org.springframework.context.annotation.Bean
        org.springframework.transaction.support.TransactionTemplate outboxProTransactionTemplate(
                javax.sql.DataSource dataSource) {
            org.springframework.transaction.support.TransactionTemplate template =
                    new org.springframework.transaction.support.TransactionTemplate(
                            new org.springframework.jdbc.datasource.DataSourceTransactionManager(dataSource));
            template.setPropagationBehavior(
                    org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
            return template;
        }
    }
}
