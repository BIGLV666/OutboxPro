# OutboxPro 订单示例

10 分钟跑通 OutboxPro 全部核心能力：**事务发布 → Reliable 消费 → 失败重试 → DLQ → 人工重放**。

## 第 1 步：启动依赖（约 1 分钟）

```bash
cd outboxpro-example-order
docker compose up -d
# 等待健康检查通过（MySQL 3306 / RabbitMQ 5672，管理台 http://localhost:15672 guest/guest）
```

## 第 2 步：启动应用（约 1 分钟）

```bash
# 仓库根目录执行
mvn install -DskipTests
cd outboxpro-example-order
mvn spring-boot:run
```

应用启动时框架自动完成：OutboxPro 三张表建表、事件注册、RabbitMQ 拓扑声明（业务队列 / Retry Queue / DLQ）、消费者启动。

## 第 3 步：正常下单（Reliable + Best Effort 双消费）

```bash
curl -X POST http://localhost:8080/orders -H "Content-Type: application/json" -d '{"amount": 100}'
# {"orderId":1,"amount":100}
```

观察应用日志，同一事件被两个队列分别消费：

```text
[inventory]     reserved for order 1 (eventId=...)
[notification]  order 1 created, notifying user (eventId=...)
```

同时可以验证：`orders` 表新增一行、`outboxpro_outbox` 表该事件状态为 `SENT`、
`outboxpro_inbox` 表两个 consumer 各有一条 `SUCCESS` 记录。

## 第 4 步：观察失败重试与 DLQ

```bash
curl -X POST http://localhost:8080/orders -H "Content-Type: application/json" -d '{"amount": -1}'
```

负金额订单会让库存 Handler 持续失败（演示用）。日志中可以看到：
第一次失败 → 消息进入 Retry Queue（TTL 2s，由绑定 RetryPolicy 的退避公式决定）→ 回流重试 →
第二次、第三次失败 → 耗尽后进入 DLQ，台账记录原因为 `RETRY_EXHAUSTED`。
而通知 Handler（Best Effort）不受影响，正常完成。

## 第 5 步：人工重放

查询死信台账拿到 eventId：

```bash
docker compose exec mysql mysql -uroot -proot order_demo \
  -e "SELECT event_id, reason_code, replay_count FROM outboxpro_dead_letter ORDER BY id DESC LIMIT 1"
```

修复问题后重放（示例应用为演示放行了所有操作员；生产环境必须接入真实授权）：

```bash
curl -X POST http://localhost:8080/actuator/outboxpro/dlq/{eventId}/replay \
  -H "Content-Type: application/json" \
  -d '{"operator": "ops-alice", "reason": "inventory restored"}'
```

## 项目结构

```text
src/main/java/org/outboxpro/example/order/
├── OrderExampleApplication.java        # 应用入口
├── config/OutboxProConfig.java         # 事件定义 + 两条订阅 + 重放授权器（全部业务方需要声明的配置）
├── event/OrderCreated.java             # 事件载荷
├── service/OrderService.java           # @Transactional：订单表 + Outbox 同事务写入
├── handler/InventoryHandler.java       # Reliable 消费（失败可重试、可死信、可重放）
├── handler/NotificationHandler.java    # Best Effort 消费（失败即放弃）
└── web/OrderController.java            # REST 入口
```

## 配置说明

见 `src/main/resources/application.yml`，全部配置带默认值，示例只显式声明了需要调整的项。
框架表（`outboxpro_outbox` / `outboxpro_inbox` / `outboxpro_dead_letter`）由
`outboxpro.schema-initialize=true` 自动创建；生产环境建议关闭并交给 Flyway/Liquibase 执行
`outboxpro-persistence-mysql/src/main/resources/db/migration/mysql/` 下的脚本。
