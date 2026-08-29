package org.outboxpro.core.event;

/**
 * 生产端事件定义，将事件类型、结构版本、载荷类型和 RabbitMQ 路由绑定起来。
 *
 * @param <T> 事件载荷类型
 */
public final class EventDefinition<T> {
    private final String eventType;
    private final String schemaVersion;
    private final Class<T> payloadType;
    private final EventRoute route;

    /**
     * 创建事件定义并校验其路由契约。
     *
     * @param eventType 事件类型
     * @param schemaVersion 结构版本
     * @param payloadType 载荷 Java 类型
     * @param route RabbitMQ 目标路由
     * @throws IllegalArgumentException 任一必填参数为空时抛出
     */
    public EventDefinition(String eventType, String schemaVersion, Class<T> payloadType, EventRoute route) {
        if (eventType == null || eventType.isBlank()) throw new IllegalArgumentException("eventType must not be blank");
        if (schemaVersion == null || schemaVersion.isBlank()) throw new IllegalArgumentException("schemaVersion must not be blank");
        if (payloadType == null) throw new IllegalArgumentException("payloadType must not be null");
        if (route == null) throw new IllegalArgumentException("route must not be null");
        this.eventType = eventType;
        this.schemaVersion = schemaVersion;
        this.payloadType = payloadType;
        this.route = route;
    }

    /** @param <T> 载荷类型 @return 新的事件定义 Builder。 */
    public static <T> Builder<T> builder() { return new Builder<>(); }
    /** @return 事件类型。 */
    public String getEventType() { return eventType; }
    /** @return 结构版本。 */
    public String getSchemaVersion() { return schemaVersion; }
    /** @return 载荷类型。 */
    public Class<T> getPayloadType() { return payloadType; }
    /** @return RabbitMQ 路由。 */
    public EventRoute getRoute() { return route; }

    /** 事件定义 Builder，提供业务侧友好的声明式配置方式。 */
    public static final class Builder<T> {
        private String eventType;
        private String schemaVersion = "v1";
        private Class<T> payloadType;
        private EventRoute route;

        /** @param value 事件类型 @return 当前 Builder。 */
        public Builder<T> eventType(String value) { eventType = value; return this; }
        /** @param value 结构版本 @return 当前 Builder。 */
        public Builder<T> schemaVersion(String value) { schemaVersion = value; return this; }
        /** @param value 载荷类型 @return 当前 Builder。 */
        public Builder<T> payloadType(Class<T> value) { payloadType = value; return this; }
        /** @param exchange 目标交换机 @param routingKey 路由键 @return 当前 Builder。 */
        public Builder<T> route(String exchange, String routingKey) { route = new EventRoute(exchange, routingKey); return this; }
        /** @param value 已构造的路由 @return 当前 Builder。 */
        public Builder<T> route(EventRoute value) { route = value; return this; }
        /** @return 校验后的不可变事件定义。 @throws IllegalArgumentException 配置不完整时抛出。 */
        public EventDefinition<T> build() { return new EventDefinition<>(eventType, schemaVersion, payloadType, route); }
    }
}
