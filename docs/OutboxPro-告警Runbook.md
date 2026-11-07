# OutboxPro 观测与告警 Runbook

> 适用版本：V1.4 起  
> 目标读者：值班研发、SRE、DBA、消息中间件运维  
> 适用范围：OutboxPro 的 Outbox 积压、DLQ 积压、健康检查与 Micrometer 指标

## 1. 指标清单

### 1.1 当前值 Gauge

| 指标 | 含义 | 标签 |
|---|---|---|
| `outboxpro.outbox.backlog.pending` | 当前 PENDING 待投递数量 | 无 |
| `outboxpro.outbox.backlog.processing` | 当前 PROCESSING 认领中数量 | 无 |
| `outboxpro.outbox.backlog.retry_waiting` | 当前 RETRY_WAITING 等待重试数量 | 无 |
| `outboxpro.outbox.backlog.dead` | 当前 DEAD 生产端死信数量 | 无 |
| `outboxpro.outbox.oldest_pending_age` | 最老 PENDING/RETRY_WAITING 消息年龄（秒） | 无 |
| `outboxpro.dlq.backlog.pending_replay` | 当前待人工重放死信总量 | 无 |
| `outboxpro.dlq.backlog.by_reason` | 当前待人工重放死信按原因分布 | `reason` |

### 1.2 累计 Counter

| 指标 | 含义 | 标签 |
|---|---|---|
| `outboxpro.publish.total` | Relay 发布尝试次数 | `event_type`、`producer` |
| `outboxpro.publish.success` | 发布成功次数 | `event_type`、`producer` |
| `outboxpro.publish.failure` | 发布失败次数 | `event_type`、`producer` |
| `outboxpro.relay.claimed` | Relay 认领消息总数 | 无 |
| `outboxpro.consume.total` | 消费开始次数 | `event_type`、`consumer`、`queue` |
| `outboxpro.consume.success` | 消费成功次数 | `event_type`、`consumer`、`queue` |
| `outboxpro.consume.failure` | 消费失败次数 | `event_type`、`consumer`、`queue` |
| `outboxpro.consume.retry` | 进入 Retry Queue 次数 | `event_type`、`consumer`、`queue` |
| `outboxpro.consume.dead` | 进入死信流程次数 | `event_type`、`consumer`、`queue` |
| `outboxpro.consume.dead.reason` | 成功进入死信流程的次数 | `event_type`、`consumer`、`queue`、`reason` |
| `outboxpro.inbox.duplicate` | Inbox 判定重复投递次数 | `event_type`、`consumer` |
| `outboxpro.log.sink.dropped` | 数据库日志 Sink 队列满丢弃条数 | 无 |
| `outboxpro.log.sink.flush.failed` | 数据库日志 Sink 批量写入失败条数 | 无 |

### 1.3 耗时 Timer（V1.4）

| 指标 | 含义 | 标签 |
|---|---|---|
| `outboxpro.publish.duration` | 单条发布耗时；批量模式下为每条记录从写出到 Confirm 的耗时 | `event_type`、`producer` |
| `outboxpro.publish.confirm.timeout` | Publisher Confirm 超时次数（Counter） | `event_type`、`producer` |
| `outboxpro.relay.cycle.duration` | Relay 单轮耗时（含空轮） | `claimed=true|false` |
| `outboxpro.consume.duration` | Handler 处理耗时 | `event_type`、`consumer`、`queue` |

Timer 建议关注 p95/p99 而非平均值；`claimed=false` 的 Relay 轮次可以用于评估空闲退避效果。
`outboxpro.publish.confirm.timeout` 增长通常意味着 Broker 或网络抖动，持续增长会推高
`outboxpro.publish.duration`，并可能在 `claim-timeout` 内无法完成状态回写。

### 1.4 指标采集策略

- Gauge 读取的是共享积压快照，快照默认缓存 30 秒。
- 自定义仓储未实现 `oldestPendingAgeSeconds` 时，`oldest_pending_age` Gauge 记为 0，
  健康检查详情为 `"unknown"`；不要仅凭该 Gauge 判断“仓储不支持统计”。
- Prometheus 抓取、Kubernetes 探针同时出现时，同一 TTL 窗口内只触发一次数据库聚合查询。
- 原因分布查询只扫描 `PENDING_REPLAY`，并使用 `(status, reason_code)` 复合索引。
- 累计原因 Counter 在死信分派成功时递增，不扫描历史台账。

## 2. 健康检查

默认开启，端点：

```text
GET /actuator/health
```

返回示例：

```json
{
  "status": "UP",
  "components": {
    "outboxPro": {
      "status": "UP",
      "details": {
        "outboxPending": 0,
        "outboxProcessing": 0,
        "outboxRetryWaiting": 0,
        "outboxDead": 0,
        "outboxOldestPendingAgeSeconds": 0,
        "dlqPendingReplay": 0,
        "snapshotCapturedAt": "2026-10-05T14:00:00Z",
        "outboxStatsAvailable": true,
        "dlqStatsAvailable": true
      }
    }
  }
}
```

状态判定：

| 条件 | 状态 | 说明 |
|---|---|---|
| `outboxDead >= outbox-dead-threshold` | `DOWN` | 生产端消息已进入不可自动恢复终态 |
| `dlqPendingReplay >= dlq-pending-threshold` | `DOWN` | 消费端死信需要人工处置 |
| `pending + retry_waiting >= outbox-backlog-threshold` | `OUT_OF_SERVICE` | 投递延迟超出预期，但尚未进入终态 |
| 其他 | `UP` | 未触发积压阈值 |
| 实现不支持统计 | `UP` + `"unknown"` | 不把“不支持统计”误判为故障 |
| 快照查询失败 | `DOWN` | 数据库不可访问或聚合查询失败，详情中 `outboxQueryFailed` / `dlqQueryFailed` 为 true |

配置：

```yaml
outboxpro:
  metrics:
    snapshot-ttl: 30s
    health:
      enabled: true
      outbox-backlog-threshold: 1000
      outbox-dead-threshold: 1
      dlq-pending-threshold: 100
```

## 3. 推荐告警规则

以下阈值是起始建议值，正式环境应结合业务 SLA、峰谷比和历史基线调整。

| 告警 | 表达式建议 | 严重度 | 处理入口 |
|---|---|---|---|
| 生产端 DEAD | `outboxpro_outbox_backlog_dead > 0` | P1 | 第 4.1 节 |
| 最老待投递消息超 SLA | `outboxpro_outbox_oldest_pending_age > 300` 持续 5m | P2 | 第 4.2 节 |
| Outbox PENDING 持续积压 | `outboxpro_outbox_backlog_pending > 1000` 持续 10m | P2 | 第 4.2 节 |
| Outbox RETRY_WAITING 持续增长 | `outboxpro_outbox_backlog_retry_waiting > 500` 持续 15m | P2 | 第 4.3 节 |
| DLQ 待重放积压 | `outboxpro_dlq_backlog_pending_replay > 100` 持续 10m | P1 | 第 5.1 节 |
| DLQ 原因异常集中 | `increase(outboxpro_consume_dead_reason_total{reason="MALFORMED_MESSAGE"}[5m]) > 0` | P1 | 第 5.2 节 |
| 消费失败率高 | `rate(outboxpro_consume_failure_total[5m]) / rate(outboxpro_consume_total[5m]) > 0.2` | P2 | 第 5.3 节 |
| 日志 Sink 降级 | `increase(outboxpro_log_sink_dropped_total[5m]) > 0` | P3 | 第 5.4 节 |

## 4. 生产端积压处理

### 4.1 DEAD 消息

排查步骤：

1. 查询 `outboxpro_outbox` 中 `status='DEAD'` 的记录，确认 `event_type`、`attempt_count`、`last_error_type`、`last_error_message`；
2. 确认 RabbitMQ 连接、目标 Exchange、用户权限和 Publisher Confirm 是否正常；
3. 如果是流量突发导致，先扩容 Relay 实例或提高 `producer.batch-size`；
4. 如果是 Broker 侧故障，恢复 Broker 后将 DEAD 复位为 PENDING；
5. 重放后持续观察 `outboxpro.outbox.backlog.dead` 是否回落。

可用运维端点：

```text
POST /actuator/outboxpro-ops/outbox/{eventId}/replay
```

### 4.2 PENDING 积压

1. 检查 `outboxpro.producer.relay-enabled` 是否为 `true`；
2. 检查 Relay 实例日志是否存在 `Outbox relay cycle failed`；
3. 检查 `outboxpro.outbox.backlog.processing` 是否同时升高，若是则可能是 Publisher Confirm 等待过久；
4. 检查 `producer.claim-timeout` 是否小于真实发布耗时；
5. 检查数据库连接池与 RabbitMQ 连接是否耗尽。

### 4.3 RETRY_WAITING 增长

1. 按 `last_error_type` 分组统计，确认是连接问题还是消息内容问题；
2. 检查 `outboxpro.producer.confirm-timeout` 是否过短；
3. 检查 `retry.max-delay` 与实际恢复时间是否匹配；
4. 若 TOP 错误集中且不可恢复，考虑业务侧幂等修复后走 `outboxpro-ops` 重放。

## 5. 消费端死信处理

### 5.1 待重放积压

1. 通过 `/actuator/outboxpro-ops/dlq` 按 `status=PENDING_REPLAY` 检索；
2. 按 `reason_code` 分组，确认积压来源；
3. 修复下游依赖或业务数据后，使用单条或批量重放端点；
4. 批量重放前先用 `dryRun=true` 评估命中范围；
5. 重放后观察 `outboxpro.dlq.backlog.pending_replay` 与原因分布是否回落。

### 5.2 原因码处理建议

| reason | 含义 | 处理建议 |
|---|---|---|
| `NON_RETRYABLE_EXCEPTION` | 业务明确不可重试 | 修复业务数据或策略后重放 |
| `RETRY_EXHAUSTED` | 可重试但已耗尽次数 | 检查下游稳定性与重试预算，恢复后重放 |
| `UNKNOWN_EVENT_TYPE` | 订阅未声明事件类型 | 补齐订阅或确认是否需要丢弃 |
| `MALFORMED_MESSAGE` | 报文无法解析 | 检查生产端序列化版本和消息来源 |
| `HANDLER_FAILURE` | Handler 或其调用链异常 | 查看 exception_type/message，修复后重放 |

### 5.3 消费失败

1. 检查 `outboxpro.consume.failure` 是否集中在某个 `consumer` / `queue`；
2. 检查 Handler 事务是否回滚；
3. 检查数据库连接池、下游依赖和超时配置；
4. 如果失败持续且消息不断重试，确认 `max-attempts` 和 `max-delay` 是否需要调整。

### 5.4 日志 Sink 降级

`outboxpro.log.sink.dropped` 或 `outboxpro.log.sink.flush.failed` 增加说明数据库消息日志丢写入。

处理：

1. 确认 `outboxpro_message_log` 表是否可写、是否有锁等待；
2. 检查 `observability.db-sink.queue-capacity` 与写入吞吐是否匹配；
3. 必要时临时切回 `message-log-sink=slf4j`，避免日志 Sink 继续占用资源；
4. 日志 Sink 是旁路能力，丢日志不改变消息投递与消费语义。

## 6. 性能与容量提示

- Relay 单实例吞吐基线（2000 条记录、真实 MySQL、零延迟 Publisher）：
  - 1 worker / batch 500：约 950–1000 records/s；
  - 2 worker / batch 500：约 1900 records/s；
  - 4 worker / batch 500：约 2200 records/s。
  详见 [性能基线](性能基线.md)。该数字不含 RabbitMQ 网络与 Confirm 等待。
- 若 `outboxpro.relay.cycle.duration` 的 `claimed=true` 分位持续升高，
  同时 `outboxpro.publish.duration` 正常，优先检查数据库锁等待与 `producer.workers`
  是否超过连接池容量。
- 积压快照刷新延迟在 25k PENDING Outbox + 25k 待重放死信下实测：
  - p50 ≈ 16ms；
  - p95 ≈ 18ms；
  - p99 ≈ 18ms。
- 待重放原因分布查询实测：走 `(status, reason_code)` 复合索引约 7ms，忽略索引全表扫描约 23ms。
- `snapshot-ttl` 建议保持 15s–60s；过小会让 Prometheus 抓取频率直接转化为数据库查询频率。
- 原因分布标签只允许框架稳定原因码与 `UNKNOWN`，不要把 `eventId`、异常消息或用户输入作为标签。

## 7. 相关配置

```yaml
outboxpro:
  producer:
    workers: 2
    batch-size: 100
    idle-backoff-max: 5s
  metrics:
    snapshot-ttl: 30s
    health:
      enabled: true
      outbox-backlog-threshold: 1000
      outbox-dead-threshold: 1
      dlq-pending-threshold: 100
  dlq:
    alert:
      enabled: true
      threshold: 100
      recovery-threshold: 80
      cooldown: 15m
```

`outboxpro.dlq.alert` 负责向业务通知器推送高位告警与恢复通知；
`outboxpro.metrics.health` 负责 Actuator 健康状态，两者互补，不互相替代。
