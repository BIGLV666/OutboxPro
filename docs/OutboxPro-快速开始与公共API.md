# OutboxPro 快速开始与公共 API

## 1. 先说明：OutboxPro 不是 HTTP 服务

OutboxPro 是一个 Spring Boot Starter，不会自动提供 REST Controller，也不会生成 `/send`、`/publish` 之类的 HTTP 接口。

业务代码通过 Spring Bean 使用它。V1 面向业务侧真正暴露的核心能力只有三类：

| 能力 | 使用方式 |
|---|---|
| 发布事件 | 注入 `OutboxProPublisher`，调用 `publish(...)` |
| 声明消费队列 | 声明 `OutboxProSubscription` Bean |
| 编写业务消费逻辑 | 实现 `OutboxProHandler<T>` Bean |

RabbitMQ ACK、Inbox、Outbox、重试、DLQ 和日志都由框架自动处理，不需要业务代码直接调用。

## 2. 业务侧真正调用的发布方法

`OutboxProPublisher` 的公共方法：

```java
public interface OutboxProPublisher {
    <T> EventEnvelope<T> publish(String eventType, T payload);

    <T> EventEnvelope<T> publish(
            String eventType,
            T payload,
            Map<String, Object> extensions
    );
}
```

最常用的是：

```java
publisher.publish(
        "order.created",
        new OrderCreatedPayload(order.getId())
);
```

带扩展元数据时：

```java
publisher.publish(
        "order.created",
        new OrderCreatedPayload(order.getId()),
        Map.of(
                "tenantId", tenantId,
                "source", "order-service"
        )
);
```

返回值是框架生成了 `eventId` 的 `EventEnvelope`。一般业务代码不需要使用返回值；如果需要审计，可以读取：

```java
EventEnvelope<OrderCreatedPayload> envelope = publisher.publish(
        "order.created",
        new OrderCreatedPayload(order.getId())
);
String eventId = envelope.getEventId();
```

## 3. 一个最小可用业务应用

### 3.1 Maven 依赖

```xml
<dependency>
    <groupId>io.github.biglv666</groupId>
    <artifactId>outboxpro-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

1.0.0 已发布到 Maven Central，直接依赖即可使用。

### 3.2 application.yml

```yaml
spring:
  application:
    name: order-service
  datasource:
    url: jdbc:mysql://127.0.0.1:3306/order_service?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&useSSL=false&allowPublicKeyRetrieval=true
    username: root
    password: ${MYSQL_PASSWORD}
    driver-class-name: com.mysql.cj.jdbc.Driver
  rabbitmq:
    host: 192.168.198.129
    port: 5672
    username: admin
    password: ${RABBITMQ_PASSWORD}
    virtual-host: /

outboxpro:
  enabled: true
  producer-name: ${spring.application.name}
  schema-initialize: true
  producer:
    enabled: true
    relay-enabled: true
    batch-size: 100
    poll-interval: 1s
    claim-timeout: 60s
    confirm-timeout: 10s
  consumer:
    enabled: true
    concurrency: 3
    prefetch: 50
  retry:
    enabled: true
    max-attempts: 5
    initial-delay: 1s
    multiplier: 2
    max-delay: 5m
```

`schema-initialize: true` 会自动创建：

- `outboxpro_outbox`；
- `outboxpro_inbox`；
- `outboxpro_message_log`。

### 3.3 定义事件载荷

```java
package com.example.order;

public record OrderCreatedPayload(Long orderId) {
}
```

### 3.4 注册生产事件

```java
package com.example.order;

import org.outboxpro.core.event.EventDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OrderEventDefinitions {

    @Bean
    public EventDefinition<OrderCreatedPayload> orderCreatedDefinition() {
        return EventDefinition.<OrderCreatedPayload>builder()
                .eventType("order.created")
                .schemaVersion("v1")
                .payloadType(OrderCreatedPayload.class)
                .route("order.exchange", "order.created")
                .build();
    }
}
```

这一段是生产端必须的。没有注册 `EventDefinition`，调用 `publish("order.created", ...)` 时会抛出：

```text
Event is not registered: order.created
```

### 3.5 声明消费订阅

```java
package com.example.inventory;

import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InventorySubscriptionConfiguration {

    @Bean
    public OutboxProSubscription orderSubscription() {
        return OutboxProSubscription.builder()
                .name("inventory-order-subscription")
                .exchange("order.exchange")
                .queue("inventory-service.queue")
                .consumerName("inventory-service")
                .bindings(EventBinding.reliable(
                        "order.created",
                        "order.created",
                        OrderCreatedPayload.class
                ))
                .build();
    }
}
```

应用启动时，OutboxPro 会自动声明：

```text
order.exchange
inventory-service.queue
order.exchange.retry
inventory-service.queue.retry.order.created.1 ... 5
order.exchange.dlx
inventory-service.queue.dlq
```

### 3.6 实现 Handler

```java
package com.example.inventory;

import org.outboxpro.core.context.EventContext;
import org.outboxpro.core.handler.OutboxProHandler;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OrderCreatedHandler implements OutboxProHandler<OrderCreatedPayload> {

    @Override
    public String eventType() {
        return "order.created";
    }

    @Override
    public Class<OrderCreatedPayload> payloadType() {
        return OrderCreatedPayload.class;
    }

    @Override
    @Transactional
    public void handle(EventContext<OrderCreatedPayload> context) {
        Long orderId = context.getPayload().orderId();

        // 这里只写业务逻辑，例如扣减库存、建立库存流水等。
        // 不要在这里手动 ACK、写 Inbox、发布 Retry Queue 或操作 DLQ。
        System.out.println("处理订单创建事件: " + orderId);
    }
}
```

### 3.7 在业务事务中发布

```java
package com.example.order;

import org.outboxpro.core.OutboxProPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {
    private final OutboxProPublisher publisher;
    private final OrderRepository orderRepository;

    public OrderService(OutboxProPublisher publisher, OrderRepository orderRepository) {
        this.publisher = publisher;
        this.orderRepository = orderRepository;
    }

    @Transactional
    public Long createOrder(CreateOrderCommand command) {
        Order order = orderRepository.insert(command);

        // 这一步只写 outboxpro_outbox，不直接访问 RabbitMQ。
        // 因为当前方法有 @Transactional，订单表和 Outbox 会一起提交或回滚。
        publisher.publish(
                "order.created",
                new OrderCreatedPayload(order.id())
        );
        return order.id();
    }
}
```

## 4. 业务代码不需要调用的方法

以下方法存在于框架内部，但不是业务 API：

```text
OutboxRelay.relayOnce()
RabbitMessagePublisher.publish(...)
RabbitConsumerManager.start()
RabbitConsumerManager.close()
RabbitTopologyManager.declare(...)
InboxRepository.tryStart(...)
InboxRepository.markSuccess(...)
```

这些 Bean 由自动装配和 Spring 生命周期调用。业务代码不应注入它们来手动驱动消息流程。

## 5. 启动后如何确认生效

### 5.1 检查数据库

```sql
SELECT event_id, event_type, status, attempt_count
FROM outboxpro_outbox
ORDER BY id DESC
LIMIT 20;
```

正常发布后：

```text
PENDING → PROCESSING → SENT
```

### 5.2 检查 Inbox

```sql
SELECT consumer_name, event_id, event_type, status, retry_count
FROM outboxpro_inbox
ORDER BY updated_time DESC
LIMIT 20;
```

正常消费后：

```text
RECEIVED → SUCCESS
```

### 5.3 检查 RabbitMQ

应看到：

```text
inventory-service.queue
inventory-service.queue.dlq
inventory-service.queue.retry.order.created.1
...
inventory-service.queue.retry.order.created.5
```

### 5.4 检查日志

默认 Logger：

```text
OUTBOX_PRO_MESSAGE
```

## 6. 常见错误

### `NoSuchBeanDefinitionException: OutboxProPublisher`

检查：

1. 是否引入 `outboxpro-spring-boot-starter`；
2. 是否存在 `DataSource`；
3. 是否存在 `RabbitTemplate`；
4. 是否设置 `outboxpro.producer.enabled=true`；
5. 是否有其他 Bean 覆盖或排除了自动装配。

### `Event is not registered: xxx`

缺少 `EventDefinition<T>` Bean，或 `eventType` 拼写不一致。

### `No handler registered for xxx`

缺少 `OutboxProHandler<T>` Bean，或 Handler 没有被 Spring 扫描。

### `Payload type mismatch for xxx`

以下三个类型必须一致：

```text
EventDefinition.payloadType()
EventBinding.payloadType()
OutboxProHandler.payloadType()
```

## 7. 当前 V1 的边界

当前 V1 暴露的是 Java/Spring Bean API，不包含：

- HTTP 发布接口；
- Outbox 查询接口；
- DLQ 管理接口；
- 人工重放接口；
- 管理后台。

这些属于后续管理平面功能，当前应通过数据库、RabbitMQ 管理台和应用日志排查。
