package org.outboxpro.spi.deadletter;

/**
 * 人工死信重放的授权边界。
 *
 * <p>实现方通常从当前 Web/Spring Security 上下文读取真实身份和权限；请求体中的 operator 仅用于审计，
 * 不能作为授权依据。</p>
 */
@FunctionalInterface
public interface DlqReplayAuthorizer {
    /**
     * 校验当前调用方是否可以重放指定事件。
     *
     * @param eventId 请求重放的事件 ID
     * @param operator 写入审计记录的操作者名称
     * @throws SecurityException 没有权限时抛出
     */
    void authorize(String eventId, String operator);
}
