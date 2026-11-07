# Changelog

本文件记录 OutboxPro 面向使用者的重要变更。版本号遵循语义化版本：
公共 API 的兼容性变更会提升次版本或主版本，纯缺陷修复提升补丁版本。

## [1.4.0] - 2026-11-07

V1.4 聚焦 Relay 容量与稳定性：单实例多工作线程、空闲指数退避、发布/消费耗时与
最老积压指标、可复现的性能基线，以及多 worker 场景下暴露的租约恢复锁竞争修复。
本版本同时包含 1.1.0 之后累积的 V1.2、V1.3 能力，统一以 1.4.0 发布。

### 新增

#### Relay 容量

- `outboxpro.producer.workers`（默认 `1`，范围 1–64）：单实例启动多个 Relay 工作线程，
  依赖 MySQL `FOR UPDATE SKIP LOCKED` 保证各线程认领的记录互不重叠。
- `outboxpro.producer.idle-backoff-max`（默认 `5s`）：连续空轮时轮询间隔按指数退避，
  上限为该值；认领到消息后立即恢复到 `producer.poll-interval`，降低多实例空闲时的数据库压力。
- 启动期校验 `producer.workers`、`producer.batch-size`（上限 10000）范围，以及
  `producer.confirm-timeout` 必须短于 `producer.claim-timeout`。

#### 观测

- 发布耗时 Timer：`outboxpro.publish.duration`，标签 `event_type`、`producer`。
- Publisher Confirm 超时 Counter：`outboxpro.publish.confirm.timeout`，标签同上。
- Relay 单轮耗时 Timer：`outboxpro.relay.cycle.duration`，标签 `claimed=true|false`，
  用于区分空轮与有效轮、定位慢轮。
- 消费耗时 Timer：`outboxpro.consume.duration`，标签 `event_type`、`consumer`、`queue`。
- 最老待投递消息年龄 Gauge：`outboxpro.outbox.oldest_pending_age`（秒），
  健康检查详情同步暴露 `outboxOldestPendingAgeSeconds`；仓储未实现该查询时降级为空值。
- `OutboxRepository.oldestPendingAgeSeconds(Instant)` 新增 default 方法，
  旧的自定义仓储实现保持二进制兼容。

#### 文档与工程

- 新增 [性能基线](docs/性能基线.md)：记录真实 MySQL + 零延迟 Publisher 下的
  batch size 与 worker 扩展曲线，以及复现方式。
- 新增 `OutboxRelayThroughputTest` 性能测试（`@Tag("performance")`，默认不执行）。
- 新增多 worker 并发投递集成测试与租约恢复节流回归测试。

### 修复

- 修复多 worker 下的租约恢复锁竞争：此前每个 Relay 每轮都执行
  `recoverExpiredClaims` 的范围 UPDATE，在 MySQL REPEATABLE READ 下产生间隙锁，
  与并发认领互相阻塞；4 worker 基准曾从约 2200 records/s 退化到 67 records/s。
  现在恢复操作按半个租约周期节流，并将认领与恢复事务显式设为
  `READ_COMMITTED`，避免范围更新持有间隙锁。
- `OutboxRelayScheduler` 的工作线程异常不再终止整个调度器：单轮异常按空闲轮处理并继续调度。

### 升级说明

- 默认配置行为与 1.3 一致（`workers=1`、空闲退避上限 5s），升级无需修改配置。
- 新增指标为低基数计数器/Timer，未接入 Micrometer 时自动降级为 no-op。
- 自定义 `OutboxRepository` 无需改动；如需最老积压指标，覆盖
  `oldestPendingAgeSeconds` 即可。
- `confirm-timeout` 大于等于 `claim-timeout` 的配置现在会在启动期失败；请确保
  `claim-timeout` 大于正常发布耗时。

## 1.1.0 之后的累积变更

### V1.3（2026-10-05）

- Outbox 积压 Gauge：`pending`、`processing`、`retry_waiting`、`dead`。
- DLQ 待重放积压 Gauge 与按原因分布 Gauge，累计死信原因 Counter。
- `OutboxProHealthIndicator`：`/actuator/health` 暴露 `outboxPro` 组件，
  阈值可配置；Gauge 与健康检查共享 TTL 快照，避免监控抓取放大数据库压力。
- 新增 `(status, reason_code)` 复合索引迁移与观测性能测试。

### V1.2（2026-09-29）

- 事务模板启动期契约校验：业务事务模板配置不满足 Outbox 写入契约时启动即失败。
- 死信批量重放端点：运维查询平面支持批量重放待处理死信。
- 数据库日志 Sink 降级指标：`outboxpro.log.sink.dropped`、
  `outboxpro.log.sink.flush.failed`。

### 修复（2026-09-27）

- RabbitMQ 发布与消费加固，Retention 任务、Inbox 陈旧 `RECEIVED` 处理，
  配置校验补强与集成测试回归。

## [1.1.0] - 2026-09-02

- 注解式声明：`@OutboxEvent`、`@OutboxHandler`、`AnnotatedOutboxHandler`。
- 类型安全发布：`publish(Class<T>, T[, extensions])`。
- `@NonRetryable` 异常标注。
- 运维查询端点 `/actuator/outboxpro-ops`（默认关闭）。
- 事件级重试策略覆盖。
- 全链路 Trace Header 传播与 Micrometer 指标门面。

## [1.0.0] - 2026-08

- 首次发布：MySQL Outbox、RabbitMQ Publisher Confirm、Inbox 幂等、Retry Queue 与 DLQ。
