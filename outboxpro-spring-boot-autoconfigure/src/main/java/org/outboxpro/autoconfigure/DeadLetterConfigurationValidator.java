package org.outboxpro.autoconfigure;

/**
 * 启动阶段校验死信相关配置的轻量 Bean。
 *
 * <p>该校验器不依赖 DataSource、Actuator 或死信仓储，确保配置冲突不会因为条件装配而被静默忽略。</p>
 */
public final class DeadLetterConfigurationValidator {
    /**
     * 校验死信配置。
     *
     * @param properties OutboxPro 配置
     */
    public DeadLetterConfigurationValidator(OutboxProProperties properties) {
        if (properties == null || properties.getDlq() == null) {
            throw new IllegalStateException("outboxpro.dlq configuration must not be null");
        }
        properties.getDlq().validate();
    }
}
