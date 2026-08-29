package org.outboxpro.transport;

/**
 * 传输层模块标记类。
 *
 * <p>本模块仅承载依赖聚合与传输相关契约的边界说明；
 * 具体的 RabbitMQ 实现位于 outboxpro-transport-rabbit。</p>
 */
public final class TransportModule {

    private TransportModule() {
        // 纯标记类，禁止实例化
    }
}
