package org.outboxpro.example.order.web;

import org.outboxpro.example.order.service.OrderService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 下单接口：POST /orders 触发"业务表 + Outbox"同事务写入。
 */
@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /** 创建订单。请求体示例：{"amount": 100}；负金额用于演示消费重试与 DLQ。 */
    @PostMapping
    public Map<String, Object> create(@RequestBody Map<String, BigDecimal> request) {
        BigDecimal amount = request.get("amount");
        if (amount == null) {
            throw new IllegalArgumentException("amount is required, e.g. {\"amount\": 100}");
        }
        long orderId = orderService.createOrder(amount);
        return Map.of("orderId", orderId, "amount", amount);
    }

    @GetMapping("/{id}")
    public Map<String, Object> get(@PathVariable long id) {
        String status = orderService.getStatus(id);
        if (status == null) {
            throw new IllegalArgumentException("order " + id + " not found");
        }
        return Map.of("orderId", id, "status", status);
    }
}
