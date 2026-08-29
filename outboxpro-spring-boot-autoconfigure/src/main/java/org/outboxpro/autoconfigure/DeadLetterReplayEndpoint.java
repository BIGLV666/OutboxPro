package org.outboxpro.autoconfigure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.outboxpro.spi.deadletter.DeadLetterRecord;
import org.outboxpro.spi.deadletter.DeadLetterRepository;
import org.outboxpro.spi.deadletter.DlqReplayAuthorizer;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.actuate.endpoint.web.annotation.RestControllerEndpoint;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 死信重放管理端点。
 *
 * <p>暴露精确路线图路径 {@code POST /actuator/outboxpro/dlq/{eventId}/replay}。
 * 端点只有在配置开启、台账可用且授权器允许当前调用方时才会执行重放。
 * 使用 {@code @RestControllerEndpoint} 而不是 {@code @ControllerEndpoint}：
 * 后者的方法返回值会被当作视图名解析，调用方拿到 404 而重放实际已执行。</p>
 */
@RestControllerEndpoint(id = "outboxpro")
public final class DeadLetterReplayEndpoint {
    private static final long REPUBLISH_CONFIRM_TIMEOUT_SECONDS = 10;
    private static final int MAX_EVENT_ID_LENGTH = 100;
    private static final int MAX_OPERATOR_LENGTH = 200;
    private static final int MAX_REASON_LENGTH = 1000;

    private final DeadLetterRepository deadLetterRepository;
    private final DlqReplayAuthorizer authorizer;
    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;
    private final int maxReplayCount;
    private final String ownerId;

    /**
     * 创建死信重放端点。
     *
     * @param deadLetterRepository 死信台账仓储
     * @param authorizer 重放授权器
     * @param rabbitTemplate 用于重新发布消息
     * @param objectMapper 从台账 payload 提取链路标识的 JSON 编解码器
     * @param maxReplayCount 单条记录最大重放次数
     * @param ownerId 当前应用实例的重放租约标识
     */
    public DeadLetterReplayEndpoint(DeadLetterRepository deadLetterRepository,
                                    DlqReplayAuthorizer authorizer,
                                    RabbitTemplate rabbitTemplate,
                                    ObjectMapper objectMapper,
                                    int maxReplayCount,
                                    String ownerId) {
        this.deadLetterRepository = deadLetterRepository;
        this.authorizer = authorizer;
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
        this.maxReplayCount = maxReplayCount;
        this.ownerId = ownerId;
    }

    /**
     * 按事件 ID 重放死信。
     *
     * @param eventId 要重放的事件 ID
     * @param request 操作人和重放原因
     * @return 重放结果
     */
    @PostMapping("/dlq/{eventId}/replay")
    public ReplayResult replayByEventId(@PathVariable String eventId, @RequestBody ReplayRequest request) {
        validate(eventId, request);

        // operator 只用于审计；真正权限由授权器从当前安全上下文判断。
        authorizer.authorize(eventId, request.operator());
        // 先释放上次进程崩溃留下的重放租约，避免记录永久停留在 REPLAYING。
        deadLetterRepository.recoverExpiredReplays(Instant.now());
        List<DeadLetterRecord> records = deadLetterRepository.claimReplayByEventId(
                eventId, ownerId, maxReplayCount, request.operator(), request.reason(), Instant.now());
        if (records.isEmpty()) {
            return new ReplayResult(0, eventId,
                    "No pending replay records found or max replay count exceeded");
        }

        int succeeded = 0;
        int failed = 0;
        for (DeadLetterRecord record : records) {
            try {
                // 人工重放开启新一轮消费尝试，历史失败次数保留在台账而不污染 Retry Queue。
                CorrelationData correlation = new CorrelationData(UUID.randomUUID().toString());
                rabbitTemplate.convertAndSend(record.originalExchange(), record.originalRoutingKey(),
                        record.payloadJson().getBytes(StandardCharsets.UTF_8), message -> {
                            message.getMessageProperties().setHeader("x-outboxpro-attempt", 1);
                            message.getMessageProperties().setHeader("x-outboxpro-replayed", true);
                            message.getMessageProperties().setHeader("x-outboxpro-replay-operator", request.operator());
                            message.getMessageProperties().setHeader("x-outboxpro-original-dead-letter-id", record.id());
                            applyOriginalTraceContext(message, record.payloadJson());
                            return message;
                        }, correlation);

                // 只有目标端 Publisher Confirm 成功后，才把台账标记为 REPLAYED。
                if (correlation.getFuture().get(REPUBLISH_CONFIRM_TIMEOUT_SECONDS, TimeUnit.SECONDS).isAck()) {
                    deadLetterRepository.markReplaySucceeded(record.id(), ownerId, Instant.now());
                    succeeded++;
                } else {
                    deadLetterRepository.releaseReplay(record.id(), ownerId,
                            "RabbitMQ rejected replay message", Instant.now());
                    failed++;
                }
            } catch (Exception error) {
                // Confirm 失败或状态更新失败时释放租约，允许后续人工重试。
                deadLetterRepository.releaseReplay(record.id(), ownerId, truncate(error.getMessage()), Instant.now());
                failed++;
            }
        }

        return new ReplayResult(succeeded, eventId,
                String.format("Replayed %d record(s), %d failed", succeeded, failed));
    }

    /**
     * 从台账 payload 的 Envelope 中提取链路标识并回填到 Header，
     * 保证重放消息与原始消息在日志里可以串联同一个 traceId。
     */
    private void applyOriginalTraceContext(org.springframework.amqp.core.Message message, String payloadJson) {
        try {
            JsonNode root = objectMapper.readTree(payloadJson);
            message.getMessageProperties().setHeader("x-outboxpro-trace-id", textOrNull(root, "traceId"));
            message.getMessageProperties().setHeader("x-outboxpro-correlation-id", textOrNull(root, "correlationId"));
            message.getMessageProperties().setHeader("x-outboxpro-causation-id", textOrNull(root, "causationId"));
        } catch (Exception ignored) {
            // 链路回填是旁路能力，解析失败不得阻断重放主流程。
        }
    }

    /** 读取 JSON 字段文本值，缺失或为空时返回 null。 */
    private String textOrNull(JsonNode root, String field) {
        JsonNode value = root.get(field);
        return value == null || value.isNull() || value.asText().isBlank() ? null : value.asText();
    }

    /** 校验路径参数和审计字段，避免超长输入进入 SQL、日志或消息 Header。 */
    private void validate(String eventId, ReplayRequest request) {        if (eventId == null || eventId.isBlank() || eventId.length() > MAX_EVENT_ID_LENGTH) {
            throw new IllegalArgumentException("eventId must be between 1 and " + MAX_EVENT_ID_LENGTH + " characters");
        }
        if (request == null || request.operator() == null || request.operator().isBlank()
                || request.operator().length() > MAX_OPERATOR_LENGTH) {
            throw new IllegalArgumentException("operator must be between 1 and " + MAX_OPERATOR_LENGTH + " characters");
        }
        if (request.reason() == null || request.reason().isBlank()
                || request.reason().length() > MAX_REASON_LENGTH) {
            throw new IllegalArgumentException("reason must be between 1 and " + MAX_REASON_LENGTH + " characters");
        }
    }

    /** 限制返回给审计表的错误信息长度。 */
    private String truncate(String value) {
        return value == null ? null : value.substring(0, Math.min(value.length(), 2000));
    }

    /** 重放请求体。 */
    public record ReplayRequest(String operator, String reason) { }

    /** 重放结果。 */
    public record ReplayResult(int replayedCount, String eventId, String message) { }
}

