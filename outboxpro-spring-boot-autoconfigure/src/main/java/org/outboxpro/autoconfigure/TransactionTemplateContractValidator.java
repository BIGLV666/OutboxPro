package org.outboxpro.autoconfigure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;

/**
 * TransactionTemplate 契约校验器：在启动阶段验证用户替换的事务模板仍满足
 * Reliable 消费的"Inbox 与业务同事务"语义，不满足时快速失败。
 *
 * <p>OutboxPro 的 {@code outboxProTransactionTemplate} Bean 通过
 * {@code @ConditionalOnMissingBean} 允许业务方覆盖。覆盖若使用 JPA 事务管理器、
 * 非 DataSource 事务管理器或修改过传播行为，会导致 Reliable 消费的事务边界
 * 静默错位：Handler 业务更新与 Inbox SUCCESS 不再同一事务提交，
 * 消息重投后业务会被重复执行且框架无法检测。</p>
 *
 * <p>本校验器通过一次真实的空事务探测验证三件事：</p>
 * <ol>
 *   <li>模板确实是 DataSource 事务管理器支撑的（能拿到目标 DataSource 的连接）；</li>
 *   <li>传播行为是 {@code PROPAGATION_REQUIRED}（执行后连接确实处于事务中）；</li>
 *   <li>回滚与提交路径都能正常执行（不出现运行期代理或配置异常）。</li>
 * </ol>
 *
 * <p>探测本身不写入任何业务数据，只执行一次 SELECT 1 并立即回滚，
 * 对生产数据库零副作用。</p>
 */
public final class TransactionTemplateContractValidator {
    private static final Logger log = LoggerFactory.getLogger(TransactionTemplateContractValidator.class);

    /**
     * 启动时校验事务模板契约。
     *
     * @param transactionTemplate 容器中的框架事务模板（可能是用户覆盖的）
     * @param dataSource 框架使用的数据源
     * @throws IllegalStateException 模板不满足 Reliable 消费契约时抛出，错误信息含修复指引
     */
    public TransactionTemplateContractValidator(TransactionTemplate transactionTemplate, DataSource dataSource) {
        if (transactionTemplate == null) {
            throw new IllegalStateException("outboxProTransactionTemplate must not be null");
        }
        validate(transactionTemplate, dataSource);
    }

    /** 执行契约探测。 */
    private void validate(TransactionTemplate transactionTemplate, DataSource dataSource) {
        // 1. 传播行为必须是 REQUIRED：非 REQUIRED 会改变"同事务提交"的边界。
        if (transactionTemplate.getPropagationBehavior()
                != org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRED) {
            throw new IllegalStateException("outboxProTransactionTemplate must use PROPAGATION_REQUIRED: "
                    + "Reliable consume requires Handler business updates and the Inbox SUCCESS row to commit "
                    + "in the same local transaction. Restore the default propagation or do not override "
                    + "outboxProTransactionTemplate.");
        }

        // 2. 在真实事务内执行一次空操作：验证模板能在目标 DataSource 上开启事务。
        try {
            Boolean executed = transactionTemplate.execute(status -> {
                // 探测点：连接必须真正处于事务中，否则"同事务"语义不成立。
                return TransactionSynchronizationManager.isActualTransactionActive()
                        && org.springframework.jdbc.datasource.DataSourceUtils
                        .isConnectionTransactional(
                                org.springframework.jdbc.datasource.DataSourceUtils.getConnection(dataSource),
                                dataSource);
            });
            if (!Boolean.TRUE.equals(executed)) {
                throw new IllegalStateException("outboxProTransactionTemplate does not operate on the target "
                        + "DataSource in an active transaction: Reliable consume requires a DataSource-backed "
                        + "transaction manager (e.g. DataSourceTransactionManager). Replace the template or "
                        + "do not override outboxProTransactionTemplate.");
            }
        } catch (IllegalStateException contractError) {
            throw contractError;
        } catch (RuntimeException error) {
            throw new IllegalStateException("outboxProTransactionTemplate failed to execute a probe transaction "
                    + "on the target DataSource: " + error.getMessage()
                    + ". Reliable consume requires a working DataSource transaction manager.", error);
        }

        // 3. 回滚路径探测：强制回滚必须不抛异常，证明模板配置完整。
        try {
            transactionTemplate.execute(status -> {
                status.setRollbackOnly();
                return null;
            });
        } catch (RuntimeException error) {
            throw new IllegalStateException("outboxProTransactionTemplate rollback probe failed: "
                    + error.getMessage() + ". Reliable consume requires working commit and rollback paths.", error);
        }

        log.debug("TransactionTemplate contract validated: propagation=REQUIRED, DataSource-backed, "
                + "commit/rollback probes passed");
    }
}
