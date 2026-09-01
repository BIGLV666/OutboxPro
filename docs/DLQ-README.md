# OutboxPro 死信队列 (DLQ) 功能

## 概述

OutboxPro 提供了完整的死信队列处理方案，包括：
- **可选死信台账**：持久化死信记录，支持审计和重放
- **分桶计数器**：固定成本的死信积压监控，不随数据膨胀而增长
- **两种处理模式**：框架发布或用户完全接管
- **HTTP 重放端点**：支持按事件 ID 批量重放，记录操作人和原因
- **告警监控**：高位告警与恢复通知

## 配置

### 基础配置

```yaml
outboxpro:
  dlq:
    # 处理模式：FRAMEWORK（框架发布 RabbitMQ DLQ）或 CUSTOM（用户完全接管）
    handling-mode: FRAMEWORK
    
    # 死信台账配置
    ledger:
      enabled: true              # 是否启用台账
      schema-initialize: true    # 是否自动建表
      counter-buckets: 32        # 分桶计数器桶数（建议 16-64）
      max-replay-count: 3        # 单条记录最大重放次数
    
    # 重放端点配置
    replay:
      enabled: false             # 默认关闭，避免未授权重放
    
    # 告警配置
    alert:
      enabled: true
      poll-interval: 1m          # 轮询间隔
      threshold: 100             # 触发告警的待重放数量
      recovery-threshold: 80     # 触发恢复通知的待重放数量
      cooldown: 15m              # 告警冷却时长
```

### 死信原因分类

框架自动分类死信原因，不可由用户重写：

- `NON_RETRYABLE_EXCEPTION`：业务明确声明不可重试
- `RETRY_EXHAUSTED`：可重试但已达最大尝试次数
- `UNKNOWN_EVENT_TYPE`：当前订阅未声明该事件类型
- `MALFORMED_MESSAGE`：消息无法解析为合法 JSON Envelope
- `HANDLER_FAILURE`：Handler 或其调用链抛出未分类异常

## 使用模式

### 1. 框架模式（FRAMEWORK）

框架自动发布 RabbitMQ DLQ，写入台账，用户可选实现旁路通知：

```java
@Component
public class CustomAlertNotifier implements DeadLetterAlertNotifier {
    
    @Override
    public void onHighWatermark(long pendingCount, long threshold) {
        // 发送高位告警：钉钉、邮件、短信等
        log.warn("DLQ 积压达到告警阈值: pending={}, threshold={}", pendingCount, threshold);
    }
    
    @Override
    public void onRecovered(long pendingCount, long recoveryThreshold) {
        // 发送恢复通知
        log.info("DLQ 积压已恢复: pending={}, recoveryThreshold={}", pendingCount, recoveryThreshold);
    }
    
    @Override
    public void notify(DeadLetterContext context) {
        // 可选：死信已可靠写入 RabbitMQ DLQ 后的旁路通知
        // 例如：归档到对象存储、发送工单等
    }
}
```

### 2. 自定义模式（CUSTOM）

用户完全接管死信发布，可转发到其他消息系统、数据库或工单平台：

```yaml
outboxpro:
  dlq:
    handling-mode: CUSTOM
```

```java
@Component
public class CustomDeadLetterStrategy implements DeadLetterStrategy {
    
    @Override
    public DeadLetterHandlingResult handle(DeadLetterContext context) {
        try {
            // 用户自行决定如何处置死信
            // 例如：写入 Kafka、调用补偿系统、发送工单等
            
            kafkaTemplate.send("dead-letter-topic", context.eventId(), context.payloadJson());
            
            // 只有在确认目标系统可靠接收后才返回 ACCEPTED
            return DeadLetterHandlingResult.ACCEPTED;
            
        } catch (Exception error) {
            // 未可靠接收，框架会 NACK + requeue 原消息
            return DeadLetterHandlingResult.REJECTED;
        }
    }
}
```

**重要**：只有用户策略返回 `ACCEPTED` 时，框架才会 ACK 原消息。返回 `REJECTED` 或抛异常会触发 `NACK + requeue`，避免消息静默丢失。

## 重放功能

### 启用重放端点

```yaml
outboxpro:
  dlq:
    ledger:
      enabled: true    # 重放依赖台账，必须启用
    replay:
      enabled: true    # 启用 HTTP 重放端点

management:
  endpoints:
    web:
      exposure:
        include: outboxpro
```

**配置强校验**：`replay.enabled=true` 时，`ledger.enabled` 必须为 `true`，否则启动失败。

### 使用重放端点

```bash
POST /actuator/outboxpro/dlq/{eventId}/replay
Content-Type: application/json

{
  "operator": "admin@example.com",
  "reason": "Manual replay after fixing downstream service"
}
```

**响应示例**：

```json
{
  "replayedCount": 2,
  "eventId": "evt-12345",
  "message": "Replayed 2 record(s), 0 failed"
}
```

### 安全建议

重放端点会产生业务副作用，建议：
1. 默认关闭（`replay.enabled=false`）
2. 配合 Spring Security 限制访问权限
3. 记录操作人和重放原因，支持审计追溯

### 死信台账与 Outbox 检索（V1.1 新增）

V1.1 提供 `@RestControllerEndpoint(id = "outboxpro-ops")` 运维端点，需要显式开启：

```yaml
outboxpro:
  ops:
    enabled: true    # 默认 false

management:
  endpoints:
    web:
      exposure:
        include: outboxpro,outboxpro-ops
```

可用接口：

```bash
# 死信台账分页检索（视图不含 payload，避免大响应）
GET /actuator/outboxpro-ops/dlq?status=PENDING_REPLAY&eventType=&consumerName=&page=0&size=20&operator=admin@example.com

# Outbox 消息分页检索
GET /actuator/outboxpro-ops/outbox?status=DEAD&eventType=&page=0&size=20&operator=admin@example.com

# Outbox 单条详情（含 payload）
GET /actuator/outboxpro-ops/outbox/{eventId}?operator=admin@example.com

# 生产端 DEAD 消息人工重放：状态复位为 PENDING，交还 Relay 重新投递
POST /actuator/outboxpro-ops/outbox/{eventId}/replay
Content-Type: application/json

{ "operator": "admin@example.com", "reason": "rabbit outage fixed" }
```

鉴权复用 `DlqReplayAuthorizer` SPI：检索与重放调用会以固定 scope
（`outbox:list` / `outbox:replay` / `dlq:list`）作为 `eventId` 参数调用 `authorize(scope, operator)`，
实现方应按 scope 判权；未配置授权器时所有调用默认拒绝。

## 性能特性

### 1. 批量认领优化

代码已优化为单次批量 UPDATE，避免逐条更新：

```sql
-- 原来：1 SELECT + N 次 UPDATE
-- 现在：1 SELECT + 1 次批量 UPDATE
UPDATE outboxpro_outbox
SET status = 'PROCESSING', ...
WHERE id IN (?, ?, ..., ?)
```

当 `batch-size=100` 时，从 101 次 SQL 减少到 2 次。

### 2. 固定成本的告警监控

使用分桶计数器实现，读取成本固定，不随死信台账数据膨胀而增长：

```sql
-- 不再扫描百万级死信表
-- 只读取固定数量的计数桶
SELECT SUM(pending_count) 
FROM outboxpro_dead_letter_counter;
```

### 3. 分派租约与崩溃恢复

台账开启时，死信进入 `DISPATCHING` 后会写入本次消费唯一的 `dispatch_owner` 和
`dispatch_until`。框架或用户策略可靠接收后，只有当前 owner 能把记录迁移为
`PENDING_REPLAY`；发布失败会立即释放租约，进程崩溃后则由过期租约允许下一次
RabbitMQ 重投递接管。活跃的 `DISPATCHING` 不会被当作“已可靠处理”直接 ACK。

### 4. 台账可关闭

当 `ledger.enabled=false` 时：
- 不创建死信台账表
- 不写入死信记录
- 不更新分桶计数器
- 不提供框架重放功能
- 用户仍可使用自定义死信策略

## 数据库表结构

### 死信台账表

```sql
CREATE TABLE outboxpro_dead_letter (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(200),
    consumer_name VARCHAR(200) NOT NULL,
    queue_name VARCHAR(255) NOT NULL,
    original_exchange VARCHAR(255) NOT NULL,
    original_routing_key VARCHAR(255) NOT NULL,
    payload_json LONGTEXT NOT NULL,
    attempt_count INT NOT NULL,
    reason_code VARCHAR(64) NOT NULL,
    reason_retryable BOOLEAN NOT NULL,
    reason_retry_exhausted BOOLEAN NOT NULL,
    exception_type VARCHAR(300),
    exception_message VARCHAR(2000),
    status VARCHAR(32) NOT NULL,
    dispatch_owner VARCHAR(100),
    dispatch_until DATETIME(6),
    replay_count INT NOT NULL DEFAULT 0,
    replay_owner VARCHAR(100),
    replayed_time DATETIME(6),
    last_replay_operator VARCHAR(200),
    last_replay_reason VARCHAR(1000),
    last_replay_error VARCHAR(2000),
    created_time DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_time DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    version BIGINT NOT NULL DEFAULT 0,
    UNIQUE KEY uk_dead_letter_event_consumer (event_id, consumer_name),
    KEY idx_dead_letter_replay (status, replay_count, id),
    KEY idx_dead_letter_event (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 分桶计数表

```sql
CREATE TABLE outboxpro_dead_letter_counter (
    counter_bucket INT NOT NULL PRIMARY KEY,
    pending_count BIGINT NOT NULL DEFAULT 0,
    updated_time DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    version BIGINT NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## 监控指标

建议监控以下指标：

1. **死信产生速率**：每分钟新增死信数量
2. **待重放积压**：`SELECT SUM(pending_count) FROM outboxpro_dead_letter_counter`
3. **死信原因分布**：按 `reason_code` 分组统计
4. **重放成功率**：成功重放 / 总重放次数
5. **告警触发频率**：高位告警与恢复通知次数

## 常见问题

### Q: 台账关闭后，死信去哪里了？

A: 框架仍会发布到 RabbitMQ DLQ（FRAMEWORK 模式）或调用用户策略（CUSTOM 模式），只是不写入 MySQL 台账。

### Q: 重放失败后会怎样？

A: 重放失败时，记录会释放租约并回到 `PENDING_REPLAY` 状态，可以再次重放，但受 `max-replay-count` 限制。

### Q: 如何防止死信无限积压？

A: 
1. 启用告警监控，及时发现积压
2. 定期清理已重放成功的历史记录
3. 分析死信原因，修复根本问题
4. 设置合理的 `max-replay-count`，避免无效重放

### Q: CUSTOM 模式下还需要 RabbitMQ DLQ 吗？

A: 不需要。CUSTOM 模式下，用户策略完全接管死信发布，可以转发到任何目标系统。

## 迁移指南

### 从无 DLQ 功能迁移

1. **升级依赖**到包含 DLQ 功能的版本
2. **添加配置**（使用默认配置即可启用）
3. **可选：实现** `DeadLetterAlertNotifier` 接收告警
4. **可选：启用重放端点**，配置访问权限

### 性能影响

- **台账启用时**：每条死信额外产生 1 次 INSERT + 1 次计数器 UPDATE
- **台账关闭时**：无额外数据库写入
- **告警任务**：固定成本，每分钟 1 次轻量查询（32 行计数桶）

---

更多详情请参考项目文档和源码注释。


### 重放授权

启用重放端点后必须提供 `DlqReplayAuthorizer` Bean。框架默认拒绝没有授权器的请求；请求体中的 `operator` 只用于审计，不能作为权限凭据。
