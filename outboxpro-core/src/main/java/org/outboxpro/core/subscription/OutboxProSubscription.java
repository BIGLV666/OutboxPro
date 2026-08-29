package org.outboxpro.core.subscription;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 用户声明的完整队列订阅，框架依据它自动创建交换机、队列、绑定、Retry Queue 和 DLQ。
 * 一个订阅对应一个业务消费队列，可以绑定多个事件类型。
 */
public final class OutboxProSubscription {
    private final String name;
    private final String exchange;
    private final String queue;
    private final String consumerName;
    private final List<EventBinding> bindings;

    public OutboxProSubscription(Builder builder) {
        this.name = required(builder.name, "name");
        this.exchange = required(builder.exchange, "exchange");
        this.queue = required(builder.queue, "queue");
        this.consumerName = builder.consumerName == null || builder.consumerName.isBlank() ? this.name : builder.consumerName;
        if (builder.bindings.isEmpty()) throw new IllegalArgumentException("bindings must not be empty");
        this.bindings = List.copyOf(builder.bindings);
    }

    private static String required(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
        return value;
    }

    /** @return 新的订阅 Builder。 */
    public static Builder builder() { return new Builder(); }
    /** @return 订阅名称。 */
    public String getName() { return name; }
    /** @return 主交换机名称。 */
    public String getExchange() { return exchange; }
    /** @return 业务消费队列名称。 */
    public String getQueue() { return queue; }
    /** @return Inbox 使用的消费者名称。 */
    public String getConsumerName() { return consumerName; }
    /** @return 不可变事件绑定列表。 */
    public List<EventBinding> getBindings() { return bindings; }

    /** 订阅 Builder。 */
    public static final class Builder {
        private String name;
        private String exchange;
        private String queue;
        private String consumerName;
        private final ArrayList<EventBinding> bindings = new ArrayList<>();

        /** @param value 订阅名称 @return 当前 Builder。 */
        public Builder name(String value) { name = value; return this; }
        /** @param value 主交换机名称 @return 当前 Builder。 */
        public Builder exchange(String value) { exchange = value; return this; }
        /** @param value 业务消费队列名称 @return 当前 Builder。 */
        public Builder queue(String value) { queue = value; return this; }
        /** @param value Inbox 消费者名称，为空时使用订阅名称 @return 当前 Builder。 */
        public Builder consumerName(String value) { consumerName = value; return this; }
        /** @param values 事件绑定列表 @return 当前 Builder。 */
        public Builder bindings(EventBinding... values) { if (values != null) Collections.addAll(bindings, values); return this; }
        /** @return 校验后的不可变订阅。 @throws IllegalArgumentException 配置不完整时抛出。 */
        public OutboxProSubscription build() { return new OutboxProSubscription(this); }
    }
}
