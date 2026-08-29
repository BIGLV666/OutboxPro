package org.outboxpro.example.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * OutboxPro 订单示例应用入口。
 *
 * <p>演示内容：业务事务中写入订单表并发布事件（Outbox 模式）、
 * Reliable 库存消费、Best Effort 通知消费、失败重试与 DLQ 人工重放。</p>
 */
@SpringBootApplication
public class OrderExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderExampleApplication.class, args);
    }
}
