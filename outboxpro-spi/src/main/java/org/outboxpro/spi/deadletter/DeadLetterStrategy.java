package org.outboxpro.spi.deadletter;

/**
 * 用户可替换的死信处置策略。
 *
 * <p>该策略可完全接管 RabbitMQ DLQ 发布，转而写入其他消息系统、数据库或工单平台；但不得修改
 * {@link DeadLetterContext#reason()} 所代表的框架原始语义。策略只有在确认目标系统可靠接收后才能返回
 * {@link DeadLetterHandlingResult#ACCEPTED}。</p>
 */
@FunctionalInterface
public interface DeadLetterStrategy {
    /**
     * 处置一条死信副本。
     *
     * @param context 不可变死信上下文
     * @return 目标系统是否已可靠接收该副本
     */
    DeadLetterHandlingResult handle(DeadLetterContext context);
}
