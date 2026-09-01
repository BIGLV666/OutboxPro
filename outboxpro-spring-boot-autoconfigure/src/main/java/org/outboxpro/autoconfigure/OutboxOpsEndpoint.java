package org.outboxpro.autoconfigure;

import org.outboxpro.spi.deadletter.DeadLetterQuery;
import org.outboxpro.spi.deadletter.DeadLetterRecord;
import org.outboxpro.spi.deadletter.DeadLetterRepository;
import org.outboxpro.spi.deadletter.DeadLetterStatus;
import org.outboxpro.spi.deadletter.DlqReplayAuthorizer;
import org.outboxpro.spi.persistence.OutboxQuery;
import org.outboxpro.spi.persistence.OutboxRecord;
import org.outboxpro.spi.persistence.OutboxRepository;
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

    /**
     * 创建运维端点。
     *
     * @param outboxRepository Outbox 仓储
     * @param deadLetterRepository 死信台账仓储
     * @param authorizer 授权器；未配置时使用默认全拒绝实现
     */
    public OutboxOpsEndpoint(OutboxRepository outboxRepository,
                             DeadLetterRepository deadLetterRepository,
                             DlqReplayAuthorizer authorizer) {
        this.outboxRepository = outboxRepository;
        this.deadLetterRepository = deadLetterRepository;
        this.authorizer = authorizer;
    }

    /**
     * 分页检索 Outbox 消息。
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

    /** 校验请求参数，避免超长输入进入 SQL、日志或审计表。 */
    private void validateRequest(String eventId, OpsRequest request) {
        if (eventId == null || eventId.isBlank() || eventId.length() > 100) {
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
