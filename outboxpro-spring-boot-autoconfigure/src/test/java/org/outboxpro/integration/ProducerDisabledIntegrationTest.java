package org.outboxpro.integration;

import org.junit.jupiter.api.Test;
import org.outboxpro.core.OutboxProPublisher;
import org.outboxpro.persistence.OutboxRelay;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 清单 T37：outboxpro.enabled=false → 不装配任何 OutboxPro Bean，应用正常启动。
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class},
        properties = "outboxpro.enabled=false")
class ProducerDisabledIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "disabled");
    }

    @Autowired
    ApplicationContext applicationContext;

    /** T37：总开关关闭时全部框架 Bean 缺席，上下文其余部分不受影响。 */
    @Test
    void disabledFlagAssemblesNoOutboxProBeans() {
        assertThat(applicationContext.getBeanNamesForType(OutboxProPublisher.class))
                .as("Publisher 不应装配").isEmpty();
        assertThat(applicationContext.getBeanNamesForType(OutboxRelay.class))
                .as("Relay 不应装配").isEmpty();
        // 注意：容器中可能存在 Boot TransactionAutoConfiguration 提供的 transactionTemplate，
        // 因此按框架 Bean 名称断言，而不是按 TransactionTemplate 类型断言。
        assertThat(applicationContext.containsBean("outboxProTransactionTemplate"))
                .as("框架事务模板不应装配").isFalse();
        // 应用其余部分不受影响：Boot 的 DataSource 自动配置正常工作
        assertThat(applicationContext.getBeanNamesForType(javax.sql.DataSource.class)).isNotEmpty();
    }
}
