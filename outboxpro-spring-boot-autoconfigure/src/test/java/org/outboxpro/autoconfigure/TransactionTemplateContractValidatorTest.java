package org.outboxpro.autoconfigure;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * {@link TransactionTemplateContractValidator} 单元测试（负路径）：
 * null 模板、非 REQUIRED 传播、非 DataSource 事务管理器三种情况必须启动失败；
 * 正路径（真实 DataSource 事务管理器）由全部集成测试上下文启动成功交叉验证。
 */
class TransactionTemplateContractValidatorTest {

    /** null 模板直接拒绝。 */
    @Test
    void nullTemplateMustFail() {
        assertThatThrownBy(() -> new TransactionTemplateContractValidator(null, mock(DataSource.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must not be null");
    }

    /** 非 REQUIRED 传播会改变 Reliable 消费的事务边界，必须在事务探测之前被拒绝。 */
    @Test
    void nonRequiredPropagationMustFailBeforeProbe() {
        TransactionTemplate template = new TransactionTemplate(
                new DataSourceTransactionManager(mock(DataSource.class)));
        template.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        assertThatThrownBy(() -> new TransactionTemplateContractValidator(template, mock(DataSource.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PROPAGATION_REQUIRED")
                .hasMessageContaining("outboxProTransactionTemplate");
    }

    /** 事务管理器无法在目标 DataSource 上开启真实事务（无激活事务上下文）时拒绝。 */
    @Test
    void nonDataSourceTransactionManagerMustFail() {
        // mock 接口的 getTransaction 返回 null：模板无法建立激活事务，探测应失败。
        TransactionTemplate template = new TransactionTemplate(mock(PlatformTransactionManager.class));
        assertThatThrownBy(() -> new TransactionTemplateContractValidator(template, mock(DataSource.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outboxProTransactionTemplate");
    }
}
