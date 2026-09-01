# OutboxPro V1 维护手册

> 适用版本：V1 / `1.1.0`  
> 当前技术范围：Java 21、Spring Boot 3.5.x、MySQL 8、RabbitMQ、Spring JDBC、Jackson  
> 文档目的：指导开发、排障、升级、发布和线上运行维护。

## 1. 维护边界

OutboxPro 的核心可靠性语义是：

```text
生产端：至少一次投递
消费端：至少一次执行 + Inbox 幂等
整体：最终一致
```

V1 不承诺：

- 跨数据库或跨 MQ 的分布式事务；
- 端到端 Exactly Once；
- 外部 HTTP、Redis、支付系统副作用的 Exactly Once；
- 自动推断业务事务边界。

业务应用必须自行保证：

1. 业务表写入和 `publisher.publish()` 在同一个本地数据库事务中；
2. Handler 的业务更新在 Reliable 消费事务中执行；
3. 外部副作用使用业务幂等键或独立去重机制。

## 2. 本地开发与构建

### 2.1 环境要求

- Java 21；
- Maven 3.9+；
- MySQL 8；
- RabbitMQ 3.x；
- Windows 开发环境下建议使用 PowerShell。

### 2.2 常用命令

```powershell
# 编译、单元测试、打包
mvn clean verify

# 安装到本地 Maven 仓库
mvn install -DskipTests

# 只运行某个模块测试
mvn -pl outboxpro-core test
mvn -pl outboxpro-persistence test

# 重新生成全部产物
mvn clean package
```

当前仓库没有提交真实密码、连接地址或运行时密钥。测试环境应通过环境变量或本地未提交配置传入。

## 3. 模块维护规则

### `outboxpro-core`

只允许放领域模型和基础契约，不得引入：

- Spring；
- RabbitMQ；
- JDBC/MyBatis；
- 具体日志实现。

修改这里会影响所有适配器，必须同步补充 Core 单元测试。

### `outboxpro-spi`

定义替换点：

- `OutboxRepository`；
- `InboxRepository`；
- `MessagePublisher`；
- `TopologyManager`；
- `EventSerializer`；
- `MessageLogSink`。

SPI 变更必须考虑二进制兼容。优先新增方法或新增接口，不要直接删除现有方法。

### `outboxpro-persistence`

放持久化无关的状态模型和 Relay。不得直接写 MySQL 方言。

### `outboxpro-persistence-mysql`

放 MySQL 8 JDBC SQL、DDL 和 Repository 实现。修改 SQL 时必须同步检查：

- 索引；
- 状态条件；
- 租约恢复；
- 乐观锁；
- MySQL 事务隔离级别；
- 大数据量下的执行计划。

### `outboxpro-transport-rabbit`

放 RabbitMQ 发布、确认、消费和拓扑。任何 ACK 时序变化都必须补充真实 RabbitMQ 集成测试。

### `outboxpro-spring-boot-autoconfigure`

放默认 Bean、属性和条件装配。默认实现必须允许用户通过 `@ConditionalOnMissingBean` 替换。

### `outboxpro-spring-boot-starter`

只聚合依赖，不放业务逻辑。

## 4. 数据库维护

DDL 文件：

`outboxpro-persistence-mysql/src/main/resources/db/migration/mysql/V1__outboxpro.sql`

### 4.1 表职责

| 表 | 作用 | 是否参与业务事务 |
|---|---|---|
| `outboxpro_outbox` | 生产消息可靠投递 | 是，必须和业务表同事务 |
| `outboxpro_inbox` | 消费幂等 | Reliable 模式下是 |
| `outboxpro_message_log` | 生命周期观测 | 否，日志失败不能影响业务 |

### 4.2 Outbox 状态

```text
PENDING
  ↓
PROCESSING
  ├── 发布确认成功 → SENT
  ├── 可重试失败 → RETRY_WAITING
  └── 超过次数 → DEAD
```

排查 Outbox 时优先查看：

```sql
SELECT id, event_id, event_type, status, attempt_count,
       next_retry_time, claim_owner, claimed_time,
       last_error_type, last_error_message,
       created_time, updated_time
FROM outboxpro_outbox
ORDER BY id DESC
LIMIT 100;
```

### 4.3 Outbox 维护操作

恢复租约超时消息应优先通过框架 Relay 完成。紧急情况下可以先确认没有仍在发布的实例，再执行：

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

人工重放 `DEAD` 消息的正式接口已于 V1.1 交付：设置 `outboxpro.ops.enabled=true` 后调用
`POST /actuator/outboxpro-ops/outbox/{eventId}/replay`（需配置 `DlqReplayAuthorizer`，
详见 DLQ-README「死信台账与 Outbox 检索」一节）。该接口把 DEAD 记录条件复位为 PENDING
（重试次数清零），由 Relay 正常认领并等待 Publisher Confirm，成功后才标记 SENT。
没有开启运维端点时仍可人工执行：

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

### 4.4 Inbox 排查

```sql
SELECT consumer_name, event_id, event_type, status,
       retry_count, last_error, received_time,
       processed_time, updated_time
FROM outboxpro_inbox
WHERE consumer_name = ?
ORDER BY updated_time DESC
LIMIT 100;
```

唯一幂等键：

```text
consumer_name + event_id
```

禁止删除 SUCCESS 记录后直接重放消息，除非已经确认业务表和外部副作用可以安全重复执行。

## 5. RabbitMQ 维护

### 5.1 拓扑命名

给定：

```text
exchange = order.exchange
queue    = inventory.queue
```

框架会创建：

```text
主 Exchange：order.exchange
主 Queue：inventory.queue
Retry Exchange：order.exchange.retry
Retry Queue：inventory.queue.retry.<eventType>.<attempt>
Dead Exchange：order.exchange.dlx
DLQ：inventory.queue.dlq
```

### 5.2 常见检查项

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

### 5.3 消费积压判断

- `Ready` 持续增长：消费者处理速度不足、消费者未启动或路由异常；
- `Unacked` 持续增长：Handler 阻塞、事务未提交、数据库连接池耗尽或 ACK 时序异常；
- Retry Queue 持续增长：业务失败或下游依赖异常；
- DLQ 持续增长：不可重试异常、配置错误或数据结构不兼容。

不要直接删除业务 Queue 来解决积压问题。删除 Queue 会丢失未处理消息。

## 6. 故障排查手册

### 6.1 业务事务成功但没有发送

检查顺序：

1. `outboxpro_outbox.status` 是否为 `PENDING`；
2. Relay 是否启用：`outboxpro.producer.relay-enabled`；
3. Relay 是否有异常日志；
4. `claim_owner` 是否长期不释放；
5. RabbitMQ 连接是否正常；
6. Publisher Confirm 是否超时；
7. 是否进入 `RETRY_WAITING` 或 `DEAD`。

### 6.2 消费者没有收到消息

检查顺序：

1. Subscription Bean 是否被 Spring 扫描；
2. Handler 的 `eventType()` 是否和 Binding 完全一致；
3. Handler 的 `payloadType()` 是否和 Binding 一致；
4. Exchange、Queue、Binding 是否成功声明；
5. 生产事件的 routing key 是否匹配；
6. RabbitMQ 用户是否有读权限；
7. 应用启动日志是否报告 Handler 配置错误。

### 6.3 Reliable Handler 反复重试

检查顺序：

1. Handler 是否抛出异常；
2. 业务事务是否回滚；
3. MySQL 连接池是否耗尽；
4. Inbox 是否可以写入；
5. 重试次数是否达到上限；
6. 是否应将异常分类为 `NonRetryableEventException`；
7. 业务副作用是否已经执行但事务回滚，是否需要业务幂等保护。

### 6.4 消息进入 DLQ

必须保留以下信息：

- eventId；
- eventType；
- producer；
- consumer；
- attempt；
- errorType；
- errorMessage；
- traceId；
- 原始消息内容或可恢复的业务主键。

处理 DLQ 前先区分：

```text
消息本身不可处理：修复数据或代码后重放
业务暂时不可用：恢复依赖后重放
消息结构不兼容：修复版本兼容策略后重放
```

## 7. 配置调优

| 配置 | 作用 | 调优建议 |
|---|---|---|
| `producer.batch-size` | Relay 单次认领量 | 先从 100 开始，观察数据库锁和 MQ 吞吐 |
| `producer.poll-interval` | Relay 轮询间隔 | 延迟敏感场景降低，数据库压力高时提高 |
| `producer.claim-timeout` | Outbox 认领租约 | 应大于正常发布耗时，避免重复认领 |
| `producer.confirm-timeout` | Publisher Confirm 超时 | 应覆盖网络抖动，但不能无限等待 |
| `consumer.concurrency` | Rabbit 消费并发 | 受 Handler CPU、数据库连接池和下游限流约束 |
| `consumer.prefetch` | Rabbit 预取数量 | 过大可能导致 Unacked 堆积 |
| `retry.max-attempts` | 消费最大次数 | 关键业务谨慎增大，避免长时间阻塞 |
| `retry.max-delay` | 最大退避时间 | 结合业务时效设置 |

调优顺序建议：

```text
先确认数据库连接池 → 再调整消费者并发 → 再调整 prefetch → 最后调整 Relay batch
```

## 8. 发布和版本维护

### 8.1 版本规则

- Core/SPI 有兼容性影响时升级主版本或次版本；
- 数据库 DDL 变化必须新增迁移版本，不修改已执行的历史迁移；
- RabbitMQ 拓扑命名变化需要提供迁移和兼容窗口；
- 事件 schema 变化必须升级 `schemaVersion`。

### 8.2 发布前检查

```text
[ ] mvn clean verify 通过
[ ] Core 单元测试通过
[ ] Relay 异常测试通过
[ ] MySQL 集成测试通过
[ ] RabbitMQ 拓扑测试通过
[ ] Reliable ACK 时序测试通过
[ ] DDL 在新库和升级库均可执行
[ ] README 和配置示例已更新
[ ] 未提交真实密码、Token 或内网敏感配置
[ ] 版本号、许可证、SCM 和发布仓库配置完整
```

## 9. 当前已知限制

P1 已于 2026-08-29 全部收官；V1.1（2026-09-02）交付以下能力，均带 Testcontainers 集成测试：

- 注解式声明：`@OutboxEvent`（载荷类）+ `@OutboxHandler`（Handler 类）+
  `AnnotatedOutboxHandler` 基类，自动生成事件定义与订阅（`AnnotationDrivenOutboxRegistrar`）；
- 类型安全发布：`publish(Class<T> payloadType, T payload[, extensions])`，按载荷类型反查唯一事件定义；
- `@NonRetryable` 异常标注：沿父类与因果链识别，命中即跳过重试直接进死信；
- 运维查询端点 `/actuator/outboxpro-ops`（默认关闭）：Outbox/死信分页检索、单条详情、
  生产端 DEAD 消息重放（`OutboxRepository`/`DeadLetterRepository` 新增 default 查询方法，
  旧自定义仓储实现保持二进制兼容）；
- 事件级重试策略：`@OutboxHandler(retry = @RetryPolicySpec(...))` 覆盖全局配置。

发布前事项：

1. 1.1.0 发布前需同步冻结 V1.1 新增公共 API（`org.outboxpro.core.annotation` 包、
   `AnnotatedOutboxHandler`、`NonRetryableExceptions`、`OutboxProPublisher` 类型安全重载、
   SPI 查询方法、`outboxpro.ops` 配置段）。

已于 2026-08-29 完成、从限制清单移除的项：

- ~~人工重放接口~~（`DeadLetterReplayEndpoint` + 死信台账已交付）；
- ~~Retry Queue TTL 线性放大~~（已改为按每个 `EventBinding` 的 `RetryPolicy` 退避公式生效：
  Retry Queue `.N` 的 TTL = `delayForAttempt(N)`，声明范围为 1..maxAttempts-1）；
- ~~部分配置缺乏启动校验~~（`OutboxProConfigurationValidator` 已覆盖全局配置 fail-fast；
  `consumer.idempotency-enabled=false` 在 V1 属于配置错误，启动即失败；
  `producer.max-retry-count` 因与 `retry.max-attempts` 语义重复已移除）；
- ~~示例应用~~（`outboxpro-example-order` 已交付，见其 README 的 10 分钟跑通指南）；
- ~~同一事件类型只能注册一个 Handler~~（`OutboxProHandler.consumerName()` 支持多消费方绑定）；
- ~~Trace Header 传播~~（已全链路打通：发布端 MDC → Outbox/Envelope → RabbitMQ Header →
  消费端 MDC（finally 清理防串号）→ Handler 日志与 MessageLog；重试与人工重放转发时回填 Header）；
- ~~Micrometer 指标~~（`OutboxMetrics` 门面 + `MicrometerOutboxMetrics` 适配器已交付：
  publish.total/success/failure、relay.claimed、consume.total/success/failure/retry/dead、
  inbox.duplicate，标签 event_type/producer/consumer/queue 低基数；无 Micrometer 时自动降级 no-op）；
- ~~数据库消息日志 Sink~~（`DatabaseMessageLogSink` 已交付：有界内存队列 + 后台批量写入
  `outboxpro_message_log`，错误信息按列宽截断，批量失败限流告警不影响主流程；
  通过 `outboxpro.observability.message-log-sink=database` 启用，默认仍为 slf4j）。

## 10. 集成测试运行指南

OutboxPro 的全部 Testcontainers 集成测试位于 `outboxpro-spring-boot-autoconfigure` 模块。

### 前置条件

- JDK 21；
- Maven 3.9+（本仓库无 wrapper，可使用 `~/.m2/wrapper/dists` 下已缓存的发行版）；
- Docker Desktop 运行中（Testcontainers 需要；无 Docker 环境下集成测试自动跳过，单元测试仍会执行）。

### 运行方式

```powershell
# 全量构建 + 全部测试（推荐入口）
mvn -pl outboxpro-spring-boot-autoconfigure -am verify

# 只跑集成测试包中的某一个类
mvn -pl outboxpro-spring-boot-autoconfigure -am test "-Dtest=ReliableConsumeIntegrationTest" "-Dsurefire.failIfNoSpecifiedTests=false"
```

注意：`-am`（also-make）不可省略。省略后 Maven 会从本地仓库解析上游模块的快照 jar，
若本地仓库中的快照早于最近一次代码变更，会出现 `NoClassDefFoundError` 等假性失败。

### 容器与隔离机制

- MySQL 8.4 与 RabbitMQ 3.13-management 容器以单例模式在整个 JVM 内共享（见
  `AbstractOutboxProIntegrationTest`），每个测试类通过 `registerIsolatedDatabase`
  获得独立数据库，避免多个缓存 Spring 上下文的 Relay 定时器互相认领记录。
- 无 Docker 时测试类通过 `@Testcontainers(disabledWithoutDocker = true)` 自动跳过。
- 首次运行需要拉取 mysql:8.4 与 rabbitmq:3.13-management 镜像，耗时较长；之后复用本地镜像。

### 排障约定

- 测试断言一律等待真实副作用（队列消息、数据库状态），使用 Awaitility 轮询；
  在 Awaitility 的 `untilAsserted` 内不要使用 `JdbcTemplate.queryForObject/queryForMap`
  查询可能不存在的行——它们抛出的 `EmptyResultDataAccessException` 不是 AssertionError，
  会立即终止等待，应改用 `queryForList` + `hasSize` 断言。
- 死信台账与分桶计数器由仓储在状态迁移时维护，测试代码不得用原始 SQL 删除台账行，
  否则 `pendingReplayCount()` 与明细对账会永久漂移；相关断言应使用相对值。
