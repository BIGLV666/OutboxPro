package org.outboxpro.spi.deadletter;

import java.time.Instant;

/**
 * 持久化的死信台账条目，保存重放所需的原消息、路由、不可变原因及最后一次操作信息。
 *
 * @param id 死信台账主键
 * @param eventId 原始事件 ID
 * @param eventType 原始事件类型
 * @param consumerName 消费者名称
 * @param queue 原消费队列
 * @param originalExchange 人工重放目标交换机
 * @param originalRoutingKey 人工重放目标路由键
 * @param payloadJson 原始消息 JSON
 * @param attemptCount 进入死信时的消费尝试次数
 * @param reason 框架确定的死信原因
 * @param status 当前死信台账状态
 * @param replayCount 已消耗的人工重放次数
 * @param replayOwner 当前重放租约持有人
 * @param replayedAt 最近一次成功重放时间
 * @param lastReplayOperator 最近一次操作人
 * @param lastReplayReason 最近一次重放原因
 * @param lastReplayError 最近一次重放失败信息
 * @param createdAt 首次死信时间
 * @param updatedAt 最近更新时间
 */
public record DeadLetterRecord(long id, String eventId, String eventType, String consumerName, String queue,
                               String originalExchange, String originalRoutingKey, String payloadJson,
                               int attemptCount, DeadLetterReason reason, DeadLetterStatus status, int replayCount,
                               String replayOwner, Instant replayedAt, String lastReplayOperator,
                               String lastReplayReason, String lastReplayError, Instant createdAt, Instant updatedAt) { }
