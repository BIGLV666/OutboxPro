# OutboxPro

[![Maven Central](https://img.shields.io/maven-central/v/io.github.biglv666/outboxpro-spring-boot-starter)](https://central.sonatype.com/artifact/io.github.biglv666/outboxpro-spring-boot-starter) [![CI](https://github.com/BIGLV666/OutboxPro/actions/workflows/ci.yml/badge.svg)](https://github.com/BIGLV666/OutboxPro/actions/workflows/ci.yml)

OutboxPro 是面向 Spring Boot 的**事务消息与 RabbitMQ 可靠消费组件**。它解决“业务已提交、消息却没发出去”或“消息重复消费”的问题：业务方只需声明事件和编写 Handler，Outbox 落库、Relay 投递、Publisher Confirm、Inbox 幂等、重试、死信与观测由框架处理。

当前 V1 支持 **Java 21 + Spring Boot 3.5.x + MySQL 8 + RabbitMQ + Spring JDBC + Jackson**。

## 核心特性

- **事务内发布**：业务表与 Outbox 表在同一本地事务提交，Relay 在事务外投递并等待 Publisher Confirm。
- **至少一次投递 + Inbox 幂等**：消费端以 `consumerName + eventId` 去重，整体最终一致。
- **注解式声明**：`@OutboxEvent` + `@OutboxHandler` 自动生成事件定义与订阅，启动期校验配置。
- **自动拓扑与重试**：Exchange、Queue、Retry Queue、DLQ 自动声明；支持事件级重试策略与 `@NonRetryable`。
- **死信台账与重放**：可选台账、分桶计数、告警通知，支持生产端 DEAD 与消费端死信人工重放。
- **可观测**：Micrometer 积压 Gauge、耗时 Timer、Confirm 超时 Counter 与 `/actuator/health` 健康检查。
- **容量与稳定性（V1.4）**：单实例多 Relay worker（`SKIP LOCKED`）、空闲指数退避、租约恢复节流。
- **可扩展**：Repository、Publisher、Serializer、拓扑、日志 Sink 等 SPI 可按需替换。

| 模块 | 职责 |
|---|---|
| `outboxpro-core` | 不依赖具体中间件的领域模型与契约 |
| `outboxpro-spi` | Repository、Publisher、Serializer、日志 Sink 等扩展点 |
| `outboxpro-persistence` | Outbox Relay 与持久化无关状态模型 |
| `outboxpro-persistence-mysql` | MySQL 8 JDBC 实现与自动建表脚本 |
| `outboxpro-transport` | 传输抽象 |
| `outboxpro-transport-rabbit` | RabbitMQ 发布确认、消费、拓扑、Retry Queue、DLQ |
| `outboxpro-observability` | 消息生命周期日志与指标适配 |
| `outboxpro-spring-boot-autoconfigure` | Spring Boot 自动装配 |
| `outboxpro-spring-boot-starter` | 业务应用开箱即用依赖 |

## Maven 坐标

```xml
<dependency>
    <groupId>io.github.biglv666</groupId>
    <artifactId>outboxpro-spring-boot-starter</artifactId>
    <version>1.4.0</version>
</dependency>
```

## 最小快速开始

应用需自行提供 Spring Boot、MySQL 与 RabbitMQ 连接配置：

```yaml
spring:
  application:
    name: order-service
  datasource:
    url: jdbc:mysql://localhost:3306/order_db
    username: root
    password: ${MYSQL_PASSWORD}
  rabbitmq:
    host: localhost
    port: 5672
    username: guest
    password: ${RABBITMQ_PASSWORD}

outboxpro:
  schema-initialize: true          # 默认自动建表；生产建议关闭并交给 Flyway/Liquibase
  producer:
    relay-enabled: true
    workers: 2
  consumer:
    concurrency: 3
```

声明事件与 Handler，然后在业务事务中发布：

```java
@OutboxEvent(eventType = "order.created", exchange = "order.exchange")
public record OrderCreated(Long orderId) { }

@Component
@OutboxHandler(event = OrderCreated.class, queue = "inventory-service.queue",
        consumerName = "inventory-service")
public class OrderCreatedHandler extends AnnotatedOutboxHandler<OrderCreated> {
    @Override public void handle(EventContext<OrderCreated> context) {
        // 只写业务逻辑；Inbox、ACK、重试、死信由框架处理
    }
}

@Transactional
public void createOrder(...) {
    orderRepository.insert(...);
    publisher.publish(OrderCreated.class, new OrderCreated(orderId));
}
```

生产环境关闭 `schema-initialize` 时，请执行
`outboxpro-persistence-mysql/src/main/resources/db/migration/mysql/` 下的 `V1`–`V6` 迁移脚本。

## 可靠性语义

- 生产端：至少一次投递；
- 消费端：至少一次执行 + Inbox 幂等；
- 整体：最终一致；
- V1 不承诺跨系统 Exactly Once，外部副作用需业务自行幂等。

## 文档导航

| 文档 | 内容 |
|---|---|
| [快速开始与公共 API](docs/OutboxPro-快速开始与公共API.md) | 完整示例、注解式声明、类型安全发布、运维端点 |
| [运维与排障](docs/OutboxPro-运维与排障.md) | 数据库、RabbitMQ、积压判断、故障排查、配置调优 |
| [观测与告警 Runbook](docs/OutboxPro-告警Runbook.md) | 指标清单、健康检查、告警规则与处理流程 |
| [死信队列 (DLQ)](docs/DLQ-README.md) | 死信模式、台账、重放、FAQ |
| [Relay 性能基线](docs/性能基线.md) | 容量结论与调优建议 |
| [全量配置示例](README-config-example.yml) | 所有配置项参考 |
| [订单示例](outboxpro-example-order/README.md) | 10 分钟跑通发布→消费→重试→DLQ→重放 |
| [变更日志](CHANGELOG.md) | 版本变更与升级说明 |

## 许可证

[Apache License 2.0](LICENSE)
