package org.outboxpro.spring.boot.starter;

/**
 * Starter 聚合模块标记类。
 *
 * <p>业务应用只需依赖 outboxpro-spring-boot-starter 即可获得
 * 自动装配、MySQL 持久化与 RabbitMQ 传输的完整能力。</p>
 */
public final class StarterModule {

    private StarterModule() {
        // 纯标记类，禁止实例化
    }
}
