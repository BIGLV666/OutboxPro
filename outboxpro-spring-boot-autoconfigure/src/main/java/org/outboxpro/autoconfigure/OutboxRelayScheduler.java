package org.outboxpro.autoconfigure;

import org.outboxpro.persistence.OutboxRelay;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.outboxpro.spi.transport.MessagePublisher;
import org.springframework.scheduling.annotation.Scheduled;

/** 触发 Relay 的轻量调度器，实际网络发布发生在认领事务之外。 */
/**
 * Outbox Relay 定时调度器，负责周期性触发投递中继。
 */
public final class OutboxRelayScheduler {
    private final OutboxRelay relay;
/**
 * 执行该公共 API 定义的操作。
 */
    public OutboxRelayScheduler(OutboxRelay relay) { this.relay = relay; }
    @Scheduled(fixedDelayString = "${outboxpro.producer.poll-interval:1000ms}")
/**
 * 执行该公共 API 定义的操作。
 */
    public void relay() { relay.relayOnce(); }
}



