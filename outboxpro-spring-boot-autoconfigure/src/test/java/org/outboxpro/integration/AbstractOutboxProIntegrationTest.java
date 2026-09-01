package org.outboxpro.integration;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * OutboxPro 集成测试基类。
 *
 * <p>采用单例容器模式：MySQL 与 RabbitMQ 容器在整个 JVM 生命周期内只启动一次，
 * 供多个 Spring 上下文（测试类）共享，显著缩短整体测试时长。
 * 容器通过静态内部类持有者延迟启动，保证在无 Docker 环境下
 * {@code @Testcontainers(disabledWithoutDocker = true)} 能先完成跳过判定，
 * 而不是在类加载阶段直接失败。</p>
 */
public abstract class AbstractOutboxProIntegrationTest {

    /** MySQL 8 容器持有者，首次访问时启动。 */
    private static final class Containers {
        static final MySQLContainer<?> MYSQL =
                new MySQLContainer<>(DockerImageName.parse("mysql:8.4"));
        static final RabbitMQContainer RABBIT =
                new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.13-management"));

        static {
            MYSQL.start();
            RABBIT.start();
        }
    }

    /** @return 共享的 MySQL 容器。 */
    static MySQLContainer<?> mysql() {
        return Containers.MYSQL;
    }

    /** @return 共享的 RabbitMQ 容器。 */
    static RabbitMQContainer rabbit() {
        return Containers.RABBIT;
    }

    /**
     * 将共享 RabbitMQ 容器连接信息注册为 Spring 属性。
     * 数据源由各测试类通过 {@link #registerIsolatedDatabase} 单独注册，实现上下文间隔离。
     *
     * @param registry Spring 动态属性注册器
     */
    @DynamicPropertySource
    static void registerContainerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.rabbitmq.host", () -> rabbit().getHost());
        registry.add("spring.rabbitmq.port", () -> rabbit().getAmqpPort());
        registry.add("spring.rabbitmq.username", () -> rabbit().getAdminUsername());
        registry.add("spring.rabbitmq.password", () -> rabbit().getAdminPassword());
        // 单例 MySQL 容器被所有 Spring 上下文共享；限制每个上下文的连接池大小，
        // 避免上下文数量增长后触发 MySQL "Too many connections"。
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> "3");
        registry.add("spring.datasource.hikari.minimum-idle", () -> "1");
    }

    /**
     * 为当前测试类创建独立数据库并注册数据源。
     *
     * <p>Spring 测试上下文在 JVM 内缓存，多个上下文的 Relay 定时器会并发轮询同一个数据库；
     * 若所有类共用一个库，其他上下文的 Relay 会把本类留下的 PENDING/RETRY_WAITING 记录
     * 抢走并投递，破坏状态机断言。按类隔离数据库可以彻底消除这类串扰。</p>
     *
     * <p>建库语句全部是字面量常量（JDBC 不支持对数据库名参数化），
     * 通过白名单 switch 确保只有预定义的库会被创建。</p>
     *
     * @param registry Spring 动态属性注册器
     * @param key 白名单中登记的测试类短键
     */
    static void registerIsolatedDatabase(DynamicPropertyRegistry registry, String key) {
        String containerJdbcUrl = mysql().getJdbcUrl();
        // 容器 JDBC URL 可能带查询参数（…/test?useSSL=false）也可能不带（…/test），
        // 两种情况都要把库名替换为隔离库名；连接服务器实例（不带库名）执行建库。
        String serverUrl = containerJdbcUrl.contains("?")
                ? containerJdbcUrl.replaceFirst("/[^/?]+\\?", "/?")
                : containerJdbcUrl.replaceFirst("/[^/?]+$", "/");
        String dbName;
        try (Connection connection = DriverManager.getConnection(serverUrl, "root", "test");
             Statement statement = connection.createStatement()) {
            dbName = switch (key) {
                case "tx" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_tx");
                    yield "outbox_it_tx";
                }
                case "e2e" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_e2e");
                    yield "outbox_it_e2e";
                }
                case "relay" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_relay");
                    yield "outbox_it_relay";
                }
                case "concurrency" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_concurrency");
                    yield "outbox_it_concurrency";
                }
                case "consume" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_consume");
                    yield "outbox_it_consume";
                }
                case "classify" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_classify");
                    yield "outbox_it_classify";
                }
                case "dlq" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_dlq");
                    yield "outbox_it_dlq";
                }
                case "alert" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_alert");
                    yield "outbox_it_alert";
                }
                case "custom" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_custom");
                    yield "outbox_it_custom";
                }
                case "topo" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_topo");
                    yield "outbox_it_topo";
                }
                case "disabled" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_disabled");
                    yield "outbox_it_disabled";
                }
                case "schemaoff" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_schemaoff");
                    yield "outbox_it_schemaoff";
                }
                case "failfast" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_failfast");
                    yield "outbox_it_failfast";
                }
                case "trace" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_trace");
                    yield "outbox_it_trace";
                }
                case "metrics" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_metrics");
                    yield "outbox_it_metrics";
                }
                case "msglog" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_msglog");
                    yield "outbox_it_msglog";
                }
                case "ext" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_ext");
                    yield "outbox_it_ext";
                }
                case "anno" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_anno");
                    yield "outbox_it_anno";
                }
                case "ops" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_ops");
                    yield "outbox_it_ops";
                }
                case "nonretry" -> {
                    statement.execute("CREATE DATABASE IF NOT EXISTS outbox_it_nonretry");
                    yield "outbox_it_nonretry";
                }
                default -> throw new IllegalArgumentException("未登记的测试数据库键: " + key);
            };
        } catch (SQLException error) {
            throw new IllegalStateException("无法创建隔离测试数据库", error);
        }
        String resolvedUrl = containerJdbcUrl.contains("?")
                ? containerJdbcUrl.replaceFirst("/[^/?]+\\?", "/" + dbName + "?")
                : containerJdbcUrl.replaceFirst("/[^/?]+$", "/" + dbName);
        registry.add("spring.datasource.url", () -> resolvedUrl);
        registry.add("spring.datasource.username", () -> "root");
        registry.add("spring.datasource.password", () -> "test");
    }
}
