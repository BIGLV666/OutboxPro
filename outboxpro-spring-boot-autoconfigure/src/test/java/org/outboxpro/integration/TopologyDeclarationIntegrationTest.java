package org.outboxpro.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.retry.RetryPolicy;
import org.outboxpro.core.subscription.ConsumeMode;
import org.outboxpro.core.subscription.EventBinding;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.outboxpro.spi.transport.TopologyManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * 清单 T13–T15：订阅拓扑声明。
 *
 * <p>使用 RabbitMQ Management HTTP API（容器自带 management 插件）直接断言
 * Broker 上的真实拓扑：exchange / queue / DLQ / Retry Queue / Retry Exchange 按订阅创建，
 * Retry Queue 的 TTL 来自绑定自身的 RetryPolicy 退避公式（initialDelay × multiplier^(N-1)），
 * 且只声明 1..maxAttempts-1 条重试队列（第 maxAttempts 次失败直接死信），重复声明幂等。</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = {IntegrationTestApplication.class, TopologyDeclarationIntegrationTest.Config.class},
        properties = {
                "outboxpro.producer.relay-enabled=false",
                "outboxpro.consumer.enabled=false",
                "outboxpro.dlq.alert.enabled=false"
        })
class TopologyDeclarationIntegrationTest extends AbstractOutboxProIntegrationTest {

    /** 本类使用独立数据库，避免其他上下文的 Relay 认领本类留下的记录。 */
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registerIsolatedDatabase(registry, "topo");
    }

    private static final String EVENT_TYPE = "it.topo.created";
    private static final String EXCHANGE = "it.topo.exchange";
    private static final String QUEUE = "it.topo.queue";
    /** 测试绑定的重试策略：initial 200ms、multiplier 3、maxAttempts 3 → retry.1 TTL=200ms、retry.2 TTL=600ms。 */
    private static final RetryPolicy TEST_RETRY_POLICY =
            new RetryPolicy(true, 3, java.time.Duration.ofMillis(200), 3, java.time.Duration.ofSeconds(5));

    @Autowired
    TopologyManager topologyManager;

    /** 测试专用配置：绑定使用可区分的退避策略（200ms × 3 倍增）。 */
    @Configuration
    static class Config {

        @Bean
        EventDefinition<OrderCreatedPayload> topoEventDefinition() {
            return EventDefinition.<OrderCreatedPayload>builder()
                    .eventType(EVENT_TYPE)
                    .payloadType(OrderCreatedPayload.class)
                    .route(EXCHANGE, EVENT_TYPE)
                    .build();
        }

        @Bean
        OutboxProSubscription topoSubscription() {
            EventBinding binding = new EventBinding(EVENT_TYPE, EVENT_TYPE, OrderCreatedPayload.class,
                    ConsumeMode.RELIABLE, TEST_RETRY_POLICY);
            return OutboxProSubscription.builder()
                    .name("it-topo")
                    .exchange(EXCHANGE)
                    .queue(QUEUE)
                    .bindings(binding)
                    .build();
        }
    }

    /** T13：主 Exchange、业务队列、DLQ、Retry Exchange、Retry Queue 全部按订阅声明创建。 */
    @Test
    void subscriptionDeclaresFullTopology() throws Exception {
        assertThat(managementGet("exchanges/%2F/" + EXCHANGE)).isNotNull();
        assertThat(managementGet("queues/%2F/" + QUEUE)).isNotNull();
        assertThat(managementGet("queues/%2F/" + QUEUE + ".dlq")).isNotNull();
        assertThat(managementGet("exchanges/%2F/" + EXCHANGE + ".retry")).isNotNull();
        assertThat(managementGet("exchanges/%2F/" + EXCHANGE + ".dlx")).isNotNull();
        assertThat(managementGet("queues/%2F/" + QUEUE + ".retry." + EVENT_TYPE + ".1")).isNotNull();
        assertThat(managementGet("queues/%2F/" + QUEUE + ".retry." + EVENT_TYPE + ".2")).isNotNull();
        // maxAttempts=3：第 3 次失败直接死信，retry.3 不应存在
        assertThat(managementGet("queues/%2F/" + QUEUE + ".retry." + EVENT_TYPE + ".3")).isNull();
    }

    /** T14：Retry Queue 的 TTL 由绑定 RetryPolicy 的退避公式决定，DLX 指回主 Exchange 与原路由键。 */
    @Test
    void retryQueueArgumentsMatchRetryPolicy() throws Exception {
        JsonNode retry1 = managementGet("queues/%2F/" + QUEUE + ".retry." + EVENT_TYPE + ".1");
        JsonNode arguments = retry1.get("arguments");

        assertThat(arguments.get("x-message-ttl").asLong())
                .as("retry.1 承载已失败 1 次的消息，TTL 应为 initialDelay=200ms")
                .isEqualTo(200L);
        assertThat(arguments.get("x-dead-letter-exchange").asText()).isEqualTo(EXCHANGE);
        assertThat(arguments.get("x-dead-letter-routing-key").asText()).isEqualTo(EVENT_TYPE);

        JsonNode retry2 = managementGet("queues/%2F/" + QUEUE + ".retry." + EVENT_TYPE + ".2");
        assertThat(retry2.get("arguments").get("x-message-ttl").asLong())
                .as("retry.2 的 TTL 应为 200ms × multiplier^1 = 600ms")
                .isEqualTo(600L);
    }

    /** T15：重复声明拓扑幂等，不抛异常（应用重启场景）。 */
    @Test
    void redeclaringTopologyIsIdempotent() {
        OutboxProSubscription subscription = topologySubscription();
        assertThatCode(() -> {
            topologyManager.declare(subscription);
            topologyManager.declare(subscription);
        }).doesNotThrowAnyException();
    }

    /** 从 Spring 容器取订阅定义有代理风险，这里直接构造等价订阅。 */
    private OutboxProSubscription topologySubscription() {
        EventBinding binding = new EventBinding(EVENT_TYPE, EVENT_TYPE, OrderCreatedPayload.class,
                ConsumeMode.RELIABLE, TEST_RETRY_POLICY);
        return OutboxProSubscription.builder()
                .name("it-topo")
                .exchange(EXCHANGE)
                .queue(QUEUE)
                .bindings(binding)
                .build();
    }

    /**
     * 调用 RabbitMQ Management HTTP API 查询实体信息。
     *
     * @param path 相对 API 路径（不含 /api/ 前缀）
     * @return 实体 JSON；404 时返回 null
     */
    private JsonNode managementGet(String path) throws Exception {
        String basic = Base64.getEncoder().encodeToString(
                (rabbit().getAdminUsername() + ":" + rabbit().getAdminPassword()).getBytes(StandardCharsets.UTF_8));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + rabbit().getHost() + ":" + rabbit().getHttpPort() + "/api/" + path))
                .header("Authorization", "Basic " + basic)
                .GET()
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 404) {
            return null;
        }
        assertThat(response.statusCode()).as("Management API 调用应成功: " + path).isEqualTo(200);
        return new ObjectMapper().readTree(response.body());
    }
}
