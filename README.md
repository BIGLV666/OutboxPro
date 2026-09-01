# OutboxPro

OutboxPro 是面向 Spring Boot 的事务消息与 RabbitMQ 操作简化组件。V1 优先支持 **MySQL + RabbitMQ + Jackson + Spring JDBC**，目标是让业务方只负责声明事件和编写 Handler。

## 文档导航

- [快速开始与公共 API](docs/OutboxPro-快速开始与公共API.md)
- [V1 维护手册](docs/OutboxPro-维护手册.md)
- [死信队列 (DLQ) 功能](docs/DLQ-README.md)
- [示例应用：10 分钟跑通全流程](outboxpro-example-order/README.md)

## 模块

- `outboxpro-core`：不可依赖具体中间件的领域契约。
- `outboxpro-spi`：Repository、Publisher、Serializer、日志 Sink 扩展点。
- `outboxpro-persistence`：Outbox Relay 与持久化模型。
- `outboxpro-persistence-mysql`：MySQL 8 JDBC 实现和自动建表脚本。
- `outboxpro-transport-rabbit`：RabbitMQ 发布确认、消费、拓扑、Retry Queue、DLQ。
- `outboxpro-observability`：消息生命周期日志。
- `outboxpro-spring-boot-autoconfigure`：Spring Boot 自动装配。
- `outboxpro-spring-boot-starter`：业务应用开箱即用依赖。

## 业务应用依赖

```xml
<dependency>
    <groupId>io.github.biglv666</groupId>
    <artifactId>outboxpro-spring-boot-starter</artifactId>
    <version>1.1.0-SNAPSHOT</version>
</dependency>
```

应用仍需要提供 Spring Boot、MySQL 和 RabbitMQ 连接配置：

```yaml
spring:
  application:
    name: order-service
  datasource:
    url: jdbc:mysql://localhost:3306/order_db?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC
    username: root
    password: ${MYSQL_PASSWORD}
  rabbitmq:
    host: localhost
    port: 5672
    username: guest
    password: ${RABBITMQ_PASSWORD}

outboxpro:
  enabled: true
  producer-name: ${spring.application.name}
  schema-initialize: true
  producer:
    relay-enabled: true
    batch-size: 100
    poll-interval: 1s
    confirm-timeout: 10s
  consumer:
    concurrency: 3
    prefetch: 50
  retry:
    max-attempts: 5
    initial-delay: 1s
    multiplier: 2
    max-delay: 5m
```

默认启动时执行 `CREATE TABLE IF NOT EXISTS`。生产环境可设置 `outboxpro.schema-initialize=false`，改由 Flyway、Liquibase 或 DBA 执行：

`outboxpro-persistence-mysql/src/main/resources/db/migration/mysql/V1__outboxpro.sql`

## 声明生产事件

```java
@Bean
public EventDefinition<OrderCreated> orderCreatedDefinition() {
    return EventDefinition.<OrderCreated>builder()
            .eventType("order.created")
            .schemaVersion("v1")
            .payloadType(OrderCreated.class)
            .route("order.exchange", "order.created")
            .build();
}
```

## 声明消费订阅

```java
@Bean
public OutboxProSubscription orderSubscription() {
    return OutboxProSubscription.builder()
            .name("order-subscription")
            .exchange("order.exchange")
            .queue("inventory-service.queue")
            .bindings(EventBinding.reliable("order.created", "order.created", OrderCreated.class))
            .build();
}
```

```java
@Component
public class OrderCreatedHandler implements OutboxProHandler<OrderCreated> {
    @Override public String eventType() { return "order.created"; }
    @Override public Class<OrderCreated> payloadType() { return OrderCreated.class; }
    @Override public void handle(EventContext<OrderCreated> context) {
        // 只编写业务逻辑；无需手动处理 Inbox、ACK、重试或日志。
    }
}
```

## 在业务事务中发布

```java
@Transactional
public void createOrder(CreateOrderCommand command) {
    Order order = saveOrder(command);
    publisher.publish("order.created", new OrderCreated(order.getId()));
    // V1.1：类型安全发布，无需手写 eventType 字符串
    // publisher.publish(OrderCreated.class, new OrderCreated(order.getId()));
}
```

业务表和 Outbox 表写入同一个本地事务；Relay 在事务外完成 RabbitMQ 发布并等待 Publisher Confirm。

## 注解式声明（V1.1，推荐）

```java
@OutboxEvent(eventType = "order.created", exchange = "order.exchange")
public record OrderCreated(Long orderId) { }

@Component
@OutboxHandler(event = OrderCreated.class, queue = "inventory-service.queue",
        consumerName = "inventory-service")
public class OrderCreatedHandler extends AnnotatedOutboxHandler<OrderCreated> {
    @Override public void handle(EventContext<OrderCreated> context) {
        // 只写业务逻辑；事件定义、订阅、Inbox、ACK、重试、死信全部由框架处理
    }
}
```

替代手写 `EventDefinition` / `OutboxProSubscription` Bean；`retry = @RetryPolicySpec(...)` 支持
事件级重试策略覆盖。详见 [快速开始与公共 API](docs/OutboxPro-快速开始与公共API.md) 第 8 节。

## 运维查询端点（V1.1，默认关闭）

```yaml
outboxpro:
  ops:
    enabled: true
```

开启后提供 `/actuator/outboxpro-ops`：Outbox 与死信台账分页检索、消息详情，
以及生产端 DEAD 消息重放（`POST /outbox/{eventId}/replay`）。鉴权复用 `DlqReplayAuthorizer` SPI，
未配置授权器时默认拒绝；详见 [DLQ 功能文档](docs/DLQ-README.md)。

## 可靠性语义

- 生产端：至少一次投递。
- 消费端：至少一次执行 + `consumerName + eventId` Inbox 幂等。
- 整体：最终一致。
- V1 不承诺跨系统 Exactly Once。

## 构建

```powershell
mvn clean verify
mvn install -DskipTests
```

当前代码要求 Java 21。

## 发布

项目已按 Maven Central（Sonatype Central）要求配置：Apache-2.0 许可证、SCM 与开发者信息、
Sources/Javadoc Jar、GPG 签名和 `central-publishing-maven-plugin` 聚合发布。

日常构建使用默认 `dev` profile（跳过签名）；发布正式版本时：

1. 在 `~/.m2/settings.xml` 配置 `<server><id>central</id>` 的用户名与密码（Central 生成的 token）；
2. 本地导入 GPG 私钥；
3. 将版本号中的 `-SNAPSHOT` 去掉后执行 `mvn -Prelease clean deploy`。


