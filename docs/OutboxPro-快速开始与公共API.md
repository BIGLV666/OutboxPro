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
    <version>1.1.0</version>
</dependency>
```

1.1.0 已发布到 Maven Central，直接依赖即可使用。

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
- 管理后台。

## 8. V1.1 新增能力

### 8.1 注解式声明（推荐）

用 `@OutboxEvent` + `@OutboxHandler` 替代手写 `EventDefinition` / `OutboxProSubscription` Bean，
同一份 eventType 字符串从 4 处重复缩减为载荷类上的 1 处声明，写错在启动时快速失败：

```java
// 载荷类：声明事件类型与路由
@OutboxEvent(eventType = "order.created", exchange = "order.exchange")
public record OrderCreatedPayload(Long orderId) {
}

// Handler：声明队列订阅，只实现业务方法
@Component
@OutboxHandler(event = OrderCreatedPayload.class, queue = "inventory-service.queue",
        consumerName = "inventory-service")   // 同一事件被多个消费方消费时必须声明
public class OrderCreatedHandler extends AnnotatedOutboxHandler<OrderCreatedPayload> {

    @Override
    public void handle(EventContext<OrderCreatedPayload> context) {
        // 只写业务逻辑；Inbox、ACK、重试、死信仍由框架处理
    }
}
```

说明：

- 载荷类必须标注 `@OutboxEvent`，否则启动失败；
- `@OutboxHandler` 的 `queue` 必填；同一队列上的多个 Handler 会合并为一个订阅；
- `retry = @RetryPolicySpec(maxAttempts = 3, initialDelayMillis = 500, ...)` 可在事件级覆盖
  全局 `outboxpro.retry`；所有字段保持默认时沿用全局配置；
- 注解式与 Builder 式 Bean 可以共存，同一 eventType 冲突注册会在启动时抛出
  `EventConfigurationException`。

### 8.2 类型安全发布

```java
// 按 payload 类型反查事件类型，无需手写字符串
publisher.publish(OrderCreatedPayload.class, new OrderCreatedPayload(orderId));

// 带 extensions 的重载
publisher.publish(OrderCreatedPayload.class, payload, Map.of("tenantId", tenantId));
```

载荷类型没有对应事件定义、或被多个事件定义共用时会抛出 `EventConfigurationException`；
后者请改用 `publish(eventType, payload)` 消除歧义。

### 8.3 @NonRetryable 异常标注

Handler 抛出标注了 `@NonRetryable` 的异常（或因果链中包含）时，框架跳过重试直接进入死信流程，
等价于抛出 `NonRetryableEventException`，但业务可以保留自定义异常类型：

```java
@NonRetryable
public class InsufficientBalanceException extends RuntimeException { ... }
```

未标注异常的行为不变：默认重试，耗尽后进死信。

### 8.4 运维查询端点（默认关闭）

设置 `outboxpro.ops.enabled=true` 后暴露 `/actuator/outboxpro-ops`：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/outbox?status=DEAD&eventType=&page=0&size=20&operator=` | Outbox 分页检索（视图不含 payload） |
| GET | `/outbox/{eventId}?operator=` | 单条详情（含 payload） |
| POST | `/outbox/{eventId}/replay` | 生产端 DEAD 消息复位为 PENDING，交还 Relay 重投 |
| GET | `/dlq?status=PENDING_REPLAY&eventType=&consumerName=&page=0&size=20&operator=` | 死信台账分页检索 |

请求体：`{"operator": "ops-alice", "reason": "bug fixed"}`。

安全边界：

- 鉴权复用 `DlqReplayAuthorizer` SPI，检索与重放以固定 scope（`outbox:list` / `outbox:replay` /
  `dlq:list`）作为 `eventId` 参数调用 `authorize(scope, operator)`，实现方按 scope 判权；
- 未配置授权器时所有调用默认拒绝；
- 端点默认关闭，列表响应不包含消息载荷，避免大响应与敏感数据外泄。
