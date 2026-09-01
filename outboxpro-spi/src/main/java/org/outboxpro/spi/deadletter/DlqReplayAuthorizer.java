package org.outboxpro.spi.deadletter;

/**
 * 人工重放与运维查询的授权边界。
 *
 * <p>实现方通常从当前 Web/Spring Security 上下文读取真实身份和权限；请求体中的 operator 仅用于审计，
 * 不能作为授权依据。</p>
 *
 * <p>{@code eventId} 参数除携带待重放的事件 ID 外，还可能携带运维检索端点的固定 scope：
 * {@code outbox:list}、{@code outbox:replay}、{@code dlq:list}。实现方应同时覆盖这两种取值，
 * 对 scope 的判权策略可以与具体事件 ID 不同（例如只允许运维角色执行重放）。</p>
 */
@FunctionalInterface
public interface DlqReplayAuthorizer {
    /**
     * 校验当前调用方是否可以重放指定事件，或访问指定 scope 的运维检索端点。
     *
     * @param eventId 请求重放的事件 ID，或运维检索 scope（outbox:list / outbox:replay / dlq:list）
     * @param operator 写入审计记录的操作者名称
     * @throws SecurityException 没有权限时抛出
     */
    void authorize(String eventId, String operator);
}
