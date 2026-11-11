# OutboxPro 运维与排障

> 目标读者：使用 OutboxPro 的研发、值班与 DBA。
> 指标告警处理见 [观测与告警 Runbook](OutboxPro-告警Runbook.md)；死信功能配置见 [DLQ 文档](DLQ-README.md)。

## 1. 可靠性语义与业务前提

```text
生产端：至少一次投递
消费端：至少一次执行 + Inbox 幂等
整体：最终一致
```

V1 不承诺跨数据库/跨 MQ 分布式事务、端到端 Exactly Once、外部 HTTP/Redis/支付副作用的 Exactly Once，也不自动推断业务事务边界。

业务应用必须保证：

1. 业务表写入和 `publisher.publish()` 在同一个本地数据库事务中；
2. Handler 的业务更新在 Reliable 消费事务中执行；
3. 外部副作用使用业务幂等键或独立去重机制。

## 2. 数据库

### 2.1 表职责

| 表 | 作用 | 是否参与业务事务 |
|---|---|---|
| `outboxpro_outbox` | 生产消息可靠投递 | 是，必须和业务表同事务 |
| `outboxpro_inbox` | 消费幂等 | Reliable 模式下是 |
| `outboxpro_message_log` | 生命周期观测 | 否，日志失败不能影响业务 |
| `outboxpro_dead_letter` | 死信台账 | 旁路，默认随框架死信流程写入 |
| `outboxpro_dead_letter_counter` | 死信分桶计数 | 旁路 |

### 2.2 建表与升级

`outboxpro.schema-initialize=true`（默认）启动时执行 `CREATE TABLE IF NOT EXISTS` 并幂等补齐索引。生产环境建议设为 `false`，改由 Flyway、Liquibase 或 DBA 执行
`outboxpro-persistence-mysql/src/main/resources/db/migration/mysql/` 下的 `V1`–`V6` 脚本：

| 脚本 | 内容 |
|---|---|
| `V1__outboxpro.sql` | 基础 Outbox/Inbox/消息日志表 |
| `V2__outboxpro_dead_letter.sql` | 死信明细与分桶计数表 |
| `V3__outboxpro_dead_letter_raw_payload.sql` | 原始载荷列扩容 |
| `V4__outboxpro_dead_letter_dispatch_lease.sql` | 死信分派租约列 |
| `V5__outboxpro_dead_letter_status_reason_index.sql` | `(status, reason_code)` 复合索引 |
| `V6__outboxpro_status_created_index.sql` | `(status, created_time)` 最老积压年龄索引 |

升级时不要修改已执行的历史迁移，新增 `V{n}__` 文件。

### 2.3 Outbox 状态

```text
PENDING
  ↓
PROCESSING
  ├── 发布确认成功 → SENT
  ├── 可重试失败 → RETRY_WAITING
  └── 超过次数 → DEAD
```

排查 Outbox：

```sql
SELECT id, event_id, event_type, status, attempt_count,
       next_retry_time, claim_owner, claimed_time,
       last_error_type, last_error_message,
       created_time, updated_time
FROM outboxpro_outbox
ORDER BY id DESC
LIMIT 100;
```

### 2.4 租约超时恢复

优先让框架 Relay 自动完成恢复。紧急情况下，先确认没有仍在发布的实例，再执行：

```sql
UPDATE outboxpro_outbox
SET status = 'PENDING',
    claim_owner = NULL,
    claimed_time = NULL,
    updated_time = NOW(),
    version = version + 1
WHERE status = 'PROCESSING'
  AND claimed_time < DATE_SUB(NOW(), INTERVAL 10 MINUTE);
```

人工重放 `DEAD` 消息优先使用运维端点（V1.1）：设置 `outboxpro.ops.enabled=true` 后调用
`POST /actuator/outboxpro-ops/outbox/{eventId}/replay`，需配置 `DlqReplayAuthorizer`，详见
[DLQ 文档](DLQ-README.md)「死信台账与 Outbox 检索」。未开启端点时可人工执行：

```sql
UPDATE outboxpro_outbox
SET status = 'PENDING',
    next_retry_time = NULL,
    claim_owner = NULL,
    claimed_time = NULL,
    updated_time = NOW(),
    version = version + 1
WHERE id = ?
  AND status = 'DEAD';
```

执行前必须记录：操作者、原因、原始错误和重放范围。

### 2.5 Inbox 排查

```sql
SELECT consumer_name, event_id, event_type, status,
       retry_count, last_error, received_time,
       processed_time, updated_time
FROM outboxpro_inbox
WHERE consumer_name = ?
ORDER BY updated_time DESC
LIMIT 100;
```

唯一幂等键：`consumer_name + event_id`。禁止删除 SUCCESS 记录后直接重放消息，除非已经确认业务表和外部副作用可以安全重复执行。

## 3. RabbitMQ

### 3.1 拓扑命名

给定 `exchange = order.exchange`、`queue = inventory.queue`，框架会创建：

```text
主 Exchange：order.exchange
主 Queue：inventory.queue
Retry Exchange：order.exchange.retry
Retry Queue：inventory.queue.retry.<eventType>.<attempt>
Dead Exchange：order.exchange.dlx
DLQ：inventory.queue.dlq
```

### 3.2 常见检查项

```text
1. RabbitMQ 连接是否可达；
2. 用户是否拥有目标 virtual host 权限；
3. Exchange、Queue、Binding 是否存在；
4. Queue 是否有消费者；
5. Ready 消息是否持续增长；
6. Unacked 是否持续增长；
7. Retry Queue 是否积压；
8. DLQ 是否出现新消息。
```

### 3.3 积压判断

- `Ready` 持续增长：消费者处理速度不足、消费者未启动或路由异常；
- `Unacked` 持续增长：Handler 阻塞、事务未提交、数据库连接池耗尽或 ACK 时序异常；
- Retry Queue 持续增长：业务失败或下游依赖异常；
- DLQ 持续增长：不可重试异常、配置错误或数据结构不兼容。

不要直接删除业务 Queue 来解决积压问题，删除 Queue 会丢失未处理消息。

## 4. 故障排查

### 4.1 业务事务成功但没有发送

1. `outboxpro_outbox.status` 是否为 `PENDING`；
2. Relay 是否启用：`outboxpro.producer.relay-enabled`；
3. Relay 是否有异常日志；
4. `claim_owner` 是否长期不释放；
5. RabbitMQ 连接是否正常；
6. Publisher Confirm 是否超时；
7. 是否进入 `RETRY_WAITING` 或 `DEAD`。

### 4.2 消费者没有收到消息

1. Subscription Bean / `@OutboxHandler` 是否被 Spring 扫描；
2. Handler 的 `eventType()` 是否和 Binding 完全一致；
3. Handler 的 `payloadType()` 是否和 Binding 一致；
4. Exchange、Queue、Binding 是否成功声明；
5. 生产事件的 routing key 是否匹配；
6. RabbitMQ 用户是否有读权限；
7. 应用启动日志是否报告 Handler 配置错误。

### 4.3 Reliable Handler 反复重试

1. Handler 是否抛出异常；
2. 业务事务是否回滚；
3. MySQL 连接池是否耗尽；
4. Inbox 是否可以写入；
5. 重试次数是否达到上限；
6. 是否应将异常分类为 `NonRetryableEventException` 或标注 `@NonRetryable`；
7. 业务副作用是否已经执行但事务回滚，是否需要业务幂等保护。

### 4.4 消息进入 DLQ

保留信息：eventId、eventType、producer、consumer、attempt、errorType、errorMessage、traceId、原始消息内容或可恢复的业务主键。

处理前先区分：

```text
消息本身不可处理：修复数据或代码后重放
业务暂时不可用：恢复依赖后重放
消息结构不兼容：修复版本兼容策略后重放
```

## 5. 配置调优

| 配置 | 作用 | 调优建议 |
|---|---|---|
| `producer.batch-size` | Relay 单次认领量 | 先从 100 开始，观察数据库锁和 MQ 吞吐；上限 10000，建议 100–500 |
| `producer.workers` | 单实例 Relay 工作线程数（1–64） | 建议 2–4；连接池至少 `workers + 1`，受 Broker Confirm 吞吐约束 |
| `producer.poll-interval` | Relay 轮询间隔 | 延迟敏感场景降低，数据库压力高时提高 |
| `producer.idle-backoff-max` | 空轮指数退避上限 | 低流量建议 5s–30s；完全空闲后首条消息最坏等待该值（受半个 `claim-timeout` 约束） |
| `producer.claim-timeout` | Outbox 认领租约 | 应大于正常发布耗时，避免重复认领 |
| `producer.confirm-timeout` | Publisher Confirm 超时 | 应覆盖网络抖动，但不能无限等待，且必须短于 `claim-timeout` |
| `consumer.concurrency` | Rabbit 消费并发 | 受 Handler CPU、数据库连接池和下游限流约束 |
| `consumer.prefetch` | Rabbit 预取数量 | 过大可能导致 Unacked 堆积 |
| `retry.max-attempts` | 消费最大次数 | 包含首次消费；关键业务谨慎增大 |
| `retry.max-delay` | 最大退避时间 | 结合业务时效设置 |
| `metrics.snapshot-ttl` | 积压观测快照缓存 | 建议 15s–60s，过小会把监控抓取频率转成数据库查询频率 |
| `metrics.health.outbox-backlog-threshold` | PENDING + RETRY_WAITING 健康阈值 | 按业务可接受的投递延迟设置 |
| `metrics.health.outbox-dead-threshold` | 生产端 DEAD 健康阈值 | 一般保持 1，出现即需人工介入 |
| `metrics.health.dlq-pending-threshold` | 待重放死信健康阈值 | 与 `dlq.alert.threshold` 配合设置 |

空闲退避行为：连续空轮时以 `poll-interval` 为基数指数退避，上限为 `idle-backoff-max`
（默认 1s → 2s → 4s → 5s）；一旦认领到消息立即恢复 `poll-interval`。系统完全空闲后到达的
首条消息最坏等待一个 `idle-backoff-max`，延迟敏感场景可设小或设为与 `poll-interval` 相同。

调优顺序：

```text
先确认数据库连接池 → 再调整 Relay workers/batch → 再调整消费者并发 → 最后调整 prefetch
```

容量基线与扩展曲线见 [性能基线](性能基线.md)；若吞吐接近 `records / claim-timeout`，优先排查锁等待或 worker 异常。

## 6. 相关文档

- [快速开始与公共 API](OutboxPro-快速开始与公共API.md)
- [死信队列 (DLQ)](DLQ-README.md)
- [观测与告警 Runbook](OutboxPro-告警Runbook.md)
- [Relay 性能基线](性能基线.md)
- [变更日志](../CHANGELOG.md)
