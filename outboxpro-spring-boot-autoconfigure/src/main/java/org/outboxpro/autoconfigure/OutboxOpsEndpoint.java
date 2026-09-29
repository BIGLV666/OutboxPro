package org.outboxpro.autoconfigure;

import org.outboxpro.spi.deadletter.DeadLetterQuery;
import org.outboxpro.spi.deadletter.DeadLetterRecord;
import org.outboxpro.spi.deadletter.DeadLetterRepository;
import org.outboxpro.spi.deadletter.DeadLetterStatus;
import org.outboxpro.spi.deadletter.DlqReplayAuthorizer;
import org.outboxpro.spi.persistence.OutboxQuery;
import org.outboxpro.spi.persistence.OutboxRecord;
import org.outboxpro.spi.persistence.OutboxRepository;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.endpoint.web.annotation.RestControllerEndpoint;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 运维查询端点：Outbox 消息检索、生产端 DEAD 重放和死信台账检索。
 *
 * <p>端点路径前缀为 {@code /actuator/outboxpro-ops}，默认关闭，
 * 需要显式设置 {@code outboxpro.ops.enabled=true} 开启。</p>
 *
 * <p>鉴权复用 {@link DlqReplayAuthorizer} SPI：检索与重放调用会以固定 scope 字符串
 * （{@code outbox:list} / {@code outbox:replay} / {@code dlq:list}）作为 eventId 参数调用
 * {@code authorize(scope, operator)}，实现方应按 scope 判权；未配置授权器时默认拒绝所有调用。</p>
 */
@RestControllerEndpoint(id = "outboxpro-ops")
public final class OutboxOpsEndpoint {

    /** 检索单页行数上限，与仓储实现的安全上限保持一致。 */
    private static final int MAX_PAGE_SIZE = 100;
    /** 允许检索的死信台账状态白名单。 */
    private static final Set<String> ALLOWED_DLQ_STATUSES = Arrays.stream(DeadLetterStatus.values())
            .map(Enum::name).collect(java.util.stream.Collectors.toUnmodifiableSet());

    private final OutboxRepository outboxRepository;
    private final DeadLetterRepository deadLetterRepository;
    private final DlqReplayAuthorizer authorizer;
    private final ObjectProvider<RetentionTask> retentionTask;
    private final ObjectProvider<DeadLetterReplayEndpoint> deadLetterReplayEndpoint;

    /**
     * 创建运维端点。
     *
     * @param outboxRepository Outbox 仓储
     * @param deadLetterRepository 死信台账仓储
     * @param authorizer 授权器；未配置时使用默认全拒绝实现
     * @param retentionTask 保留任务；手动清理复用与定时调度相同的分批删除逻辑
     * @param deadLetterReplayEndpoint 单条重放端点；批量重放按命中记录逐条复用其租约、
     *                                 次数限制与 Confirm 重发逻辑，不复制状态机
     */
    public OutboxOpsEndpoint(OutboxRepository outboxRepository,
                             DeadLetterRepository deadLetterRepository,
                             DlqReplayAuthorizer authorizer,
                             ObjectProvider<RetentionTask> retentionTask,
                             ObjectProvider<DeadLetterReplayEndpoint> deadLetterReplayEndpoint) {
        this.outboxRepository = outboxRepository;
        this.deadLetterRepository = deadLetterRepository;
        this.authorizer = authorizer;
        this.retentionTask = retentionTask;
        this.deadLetterReplayEndpoint = deadLetterReplayEndpoint;
    }

    /**
     * 分页检索 Outbox 消息。
     * 注意：total 统计与当页数据是两条独立查询，不是同一快照，清理任务运行期间可能轻微漂移。
     *
     * @param status Outbox 状态过滤（PENDING / PROCESSING / RETRY_WAITING / SENT / DEAD）
     * @param eventType 事件类型精确过滤
     * @param page 页码，从 0 开始
     * @param size 单页行数，最大 100
     * @param operator 操作人，用于授权与审计
     * @return 分页结果；payload 不在列表中返回，需要载荷时调用单条查询
     */
    @GetMapping("/outbox")
    public PageResult<OutboxMessageView> listOutbox(@RequestParam(required = false) String status,
                                                    @RequestParam(required = false) String eventType,
                                                    @RequestParam(defaultValue = "0") int page,
                                                    @RequestParam(defaultValue = "20") int size,
                                                    @RequestParam String operator) {
        authorizer.authorize("outbox:list", operator);
        OutboxQuery query = new OutboxQuery(normalizeStatus(status, OutboxQuery.ALLOWED_STATUSES),
                eventType, Math.max(page, 0) * pageSize(size), pageSize(size));
        List<OutboxMessageView> items = outboxRepository.findMessages(query).stream()
                .map(OutboxMessageView::from)
                .toList();
        return new PageResult<>(outboxRepository.countMessages(query), page, items);
    }

    /**
     * 查询单条 Outbox 消息详情（含载荷 JSON），供排障使用。
     *
     * @param eventId 事件唯一 ID
     * @param operator 操作人，用于授权与审计
     * @return 消息记录；不存在时返回 404 语义的提示对象
     */
    @GetMapping("/outbox/{eventId}")
    public Object getOutbox(@PathVariable String eventId, @RequestParam String operator) {
        authorizer.authorize("outbox:list", operator);
        OutboxRecord record = outboxRepository.findByEventId(eventId);
        return record == null ? new SimpleMessage("Outbox message not found: " + eventId) : record;
    }

    /**
     * 重放一条生产端 DEAD 消息：把状态复位为 PENDING，交还 Relay 重新投递。
     * 复位后重试预算清零；是否真正投递成功由 Relay 与 Publisher Confirm 决定。
     *
     * @param eventId 事件唯一 ID
     * @param request 操作人与原因
     * @return 复位结果
     */
    @PostMapping("/outbox/{eventId}/replay")
    public ReplayOutcome replayOutbox(@PathVariable String eventId, @RequestBody OpsRequest request) {
        validateRequest(eventId, request);
        authorizer.authorize("outbox:replay", request.operator());
        boolean reset = outboxRepository.resetDeadForReplay(eventId);
        return new ReplayOutcome(eventId, reset,
                reset ? "Outbox message reset to PENDING; relay will redeliver"
                        : "No DEAD outbox message found for this eventId");
    }

    /**
     * 分页检索死信台账。
     *
     * @param status 台账状态过滤（DISPATCHING / PENDING_REPLAY / REPLAYING / REPLAYED）
     * @param eventType 事件类型精确过滤
     * @param consumerName 消费者名称精确过滤
     * @param page 页码，从 0 开始
     * @param size 单页行数，最大 100
     * @param operator 操作人，用于授权与审计
     * @return 分页结果（不含原始载荷，避免大响应）
     */
    @GetMapping("/dlq")
    public PageResult<DeadLetterSummary> listDeadLetters(@RequestParam(required = false) String status,
                                                         @RequestParam(required = false) String eventType,
                                                         @RequestParam(required = false) String consumerName,
                                                         @RequestParam(defaultValue = "0") int page,
                                                         @RequestParam(defaultValue = "20") int size,
                                                         @RequestParam String operator) {
        authorizer.authorize("dlq:list", operator);
        DeadLetterStatus normalized = null;
        String statusName = normalizeStatus(status, ALLOWED_DLQ_STATUSES);
        if (statusName != null) {
            normalized = DeadLetterStatus.valueOf(statusName);
        }
        DeadLetterQuery query = new DeadLetterQuery(eventType, consumerName, normalized,
                Math.max(page, 0) * pageSize(size), pageSize(size));
        List<DeadLetterSummary> items = deadLetterRepository.findDeadLetters(query).stream()
                .map(DeadLetterSummary::from)
                .toList();
        return new PageResult<>(deadLetterRepository.countDeadLetters(query), page, items);
    }

    /**
     * 手动触发一轮历史数据清理：分批删除 Outbox SENT、Inbox SUCCESS、消息日志与
     * 死信台账 REPLAYED 中超过保留期的行。复用与定时调度完全相同的
     * {@link RetentionTask#purge()} 短事务分批逻辑（单语句 LIMIT 限批，不长事务锁表），
     * 清理运行中重复调用只返回 skipped。
     *
     * @param request 操作人与原因，用于授权与审计
     * @return 各表实际删除行数
     */
    @PostMapping("/retention/purge")
    public PurgeOutcome purgeRetention(@RequestBody OpsRequest request) {
        validateOperator(request);
        authorizer.authorize("retention:purge", request.operator());
        RetentionTask task = retentionTask.getIfAvailable();
        if (task == null) {
            return new PurgeOutcome(0, 0, 0, 0, true,
                    "Retention task is not available in this application (DataSource or repositories missing)");
        }
        RetentionTask.PurgeResult result = task.purge();
        return new PurgeOutcome(result.outboxSent(), result.inboxSuccess(), result.messageLog(),
                result.deadLetterReplayed(), result.skipped(),
                result.skipped() ? "Purge skipped (retention disabled or already running)"
                        : "Purge finished");
    }

    /**
     * 批量重放死信台账：按事件类型 + 消费者名称筛选 PENDING_REPLAY 记录，
     * 逐条复用单条重放的租约、次数限制与 Confirm 重发逻辑。
     *
     * <p>支持 {@code dryRun=true}：只统计命中数量，不执行任何重发，供运维先确认范围。</p>
     *
     * <p>批量重放不是原子操作：每批记录独立走单条重放流程，成功/失败分别计数并返回，
     * 失败记录保持 PENDING_REPLAY 可再次重放，不受 maxReplayCount 额外限制。</p>
     *
     * @param eventType 事件类型精确过滤，必填
     * @param consumerName 消费者名称精确过滤，必填
     * @param limit 单次最多重放的记录数，默认 100，最大 1000
     * @param dryRun 为 true 时只统计命中数量，不重发
     * @param request 操作人与原因，用于授权与审计
     * @return 命中数量、成功重放数量与失败数量
     */
    @PostMapping("/dlq/replay/batch")
    public BatchReplayOutcome replayDeadLettersBatch(@RequestParam String eventType,
                                                     @RequestParam String consumerName,
                                                     @RequestParam(defaultValue = "100") int limit,
                                                     @RequestParam(defaultValue = "false") boolean dryRun,
                                                     @RequestBody OpsRequest request) {
        validateOperator(request);
        if (eventType == null || eventType.isBlank() || eventType.length() > 200) {
            throw new IllegalArgumentException("eventType must be between 1 and 200 characters");
        }
        if (consumerName == null || consumerName.isBlank() || consumerName.length() > 200) {
            throw new IllegalArgumentException("consumerName must be between 1 and 200 characters");
        }
        authorizer.authorize("dlq:replay:batch", request.operator());

        DeadLetterReplayEndpoint replayEndpoint = deadLetterReplayEndpoint.getIfAvailable();
        if (replayEndpoint == null) {
            return new BatchReplayOutcome(0, 0, 0,
                    "Dead letter replay endpoint is not available (outboxpro.dlq.replay.enabled=false)");
        }

        int pageSize = Math.min(Math.max(limit, 1), 1000);
        DeadLetterQuery query = new DeadLetterQuery(eventType, consumerName,
                DeadLetterStatus.PENDING_REPLAY, 0, pageSize);
        List<DeadLetterRecord> matched = deadLetterRepository.findDeadLetters(query);
        long matchedCount = deadLetterRepository.countDeadLetters(query);
        if (dryRun) {
            return new BatchReplayOutcome(matchedCount, 0, 0,
                    "Dry run only; would replay " + matched.size() + " of " + matchedCount + " matched record(s)");
        }

        int succeeded = 0;
        int failed = 0;
        // 逐条复用单条重放逻辑：租约、次数限制、Confirm 与台账状态迁移保持一致。
        for (DeadLetterRecord record : matched) {
            try {
                var outcome = replayEndpoint.replayByEventId(record.eventId(),
                        new DeadLetterReplayEndpoint.ReplayRequest(request.operator(), request.reason()));
                if (outcome.replayedCount() > 0) {
                    succeeded += outcome.replayedCount();
                } else {
                    failed++;
                }
            } catch (RuntimeException error) {
                failed++;
            }
        }
        return new BatchReplayOutcome(matchedCount, succeeded, failed,
                String.format("Batch replay finished: %d succeeded, %d failed of %d matched",
                        succeeded, failed, matchedCount));
    }

    /** 校验仅含操作人/原因的请求体。 */
    private void validateOperator(OpsRequest request) {
        if (request == null || request.operator() == null || request.operator().isBlank()
                || request.operator().length() > 200) {
            throw new IllegalArgumentException("operator must be between 1 and 200 characters");
        }
        if (request.reason() == null || request.reason().isBlank() || request.reason().length() > 1000) {
            throw new IllegalArgumentException("reason must be between 1 and 1000 characters");
        }
    }

    /** 校验请求参数，避免超长输入进入 SQL、日志或审计表。 */
    private void validateRequest(String eventId, OpsRequest request) {        if (eventId == null || eventId.isBlank() || eventId.length() > 100) {
            throw new IllegalArgumentException("eventId must be between 1 and 100 characters");
        }
        if (request == null || request.operator() == null || request.operator().isBlank()
                || request.operator().length() > 200) {
            throw new IllegalArgumentException("operator must be between 1 and 200 characters");
        }
        if (request.reason() == null || request.reason().isBlank() || request.reason().length() > 1000) {
            throw new IllegalArgumentException("reason must be between 1 and 1000 characters");
        }
    }

    /** 归一化状态过滤值；空白返回 null 表示不过滤，白名单外抛出请求错误。 */
    private String normalizeStatus(String status, Set<String> allowed) {
        if (status == null || status.isBlank()) {
            return null;
        }
        String normalized = status.trim().toUpperCase(Locale.ROOT);
        if (!allowed.contains(normalized)) {
            throw new IllegalArgumentException("status must be one of " + allowed);
        }
        return normalized;
    }

    /** 页大小裁剪。 */
    private int pageSize(int size) {
        return Math.min(Math.max(size, 1), MAX_PAGE_SIZE);
    }

    /** 运维请求体。 */
    public record OpsRequest(String operator, String reason) { }

    /** 批量重放结果：命中数量、成功重放数量、失败数量与说明。 */
    public record BatchReplayOutcome(long matchedCount, int replayedCount, int failedCount, String message) { }

    /** 手动清理结果：各表删除行数与跳过标记。 */
    public record PurgeOutcome(long outboxSent, long inboxSuccess, long messageLog,
                               long deadLetterReplayed, boolean skipped, String message) { }

    /** 重放复位结果。 */
    public record ReplayOutcome(String eventId, boolean reset, String message) { }

    /** 简单提示对象。 */
    public record SimpleMessage(String message) { }

    /** 通用分页结果。 */
    public record PageResult<T>(long total, int page, List<T> items) { }

    /** Outbox 列表视图；不含 payload，避免列表接口返回大 JSON。 */
    public record OutboxMessageView(long id, String eventId, String eventType, String schemaVersion,
                                    String producer, String exchange, String routingKey, String traceId,
                                    String status, int attemptCount, Instant nextRetryTime) {
        static OutboxMessageView from(OutboxRecord record) {
            return new OutboxMessageView(record.id(), record.eventId(), record.eventType(), record.schemaVersion(),
                    record.producer(), record.exchangeName(), record.routingKey(), record.traceId(),
                    record.status(), record.attemptCount(), record.nextRetryTime());
        }
    }

    /** 死信台账列表视图；不含原始载荷，避免列表接口返回大 JSON。 */
    public record DeadLetterSummary(long id, String eventId, String eventType, String consumerName, String queue,
                                    int attemptCount, String reasonCode, String status, int replayCount,
                                    Instant replayedAt, Instant createdAt) {
        static DeadLetterSummary from(DeadLetterRecord record) {
            return new DeadLetterSummary(record.id(), record.eventId(), record.eventType(), record.consumerName(),
                    record.queue(), record.attemptCount(), record.reason().code().name(), record.status().name(),
                    record.replayCount(), record.replayedAt(), record.createdAt());
        }
    }
}
