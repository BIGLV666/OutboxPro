package org.outboxpro.core;

import org.junit.jupiter.api.Test;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.event.EventRegistry;
import org.outboxpro.core.exception.EventConfigurationException;
import org.outboxpro.core.retry.RetryPolicy;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class CoreContractTest {
    record Payload(String id) { }

    @Test
    void registryRejectsDuplicateEventType() {
        EventRegistry registry = new EventRegistry();
        var definition = EventDefinition.<Payload>builder().eventType("order.created").payloadType(Payload.class).route("orders", "order.created").build();
        registry.register(definition);
        assertThrows(EventConfigurationException.class, () -> registry.register(definition));
    }

    @Test
    void retryDelayIsBounded() {
        RetryPolicy policy = new RetryPolicy(true, 5, Duration.ofSeconds(1), 2, Duration.ofSeconds(3));
        assertEquals(Duration.ofSeconds(1), policy.delayForAttempt(1));
        assertEquals(Duration.ofSeconds(2), policy.delayForAttempt(2));
        assertEquals(Duration.ofSeconds(3), policy.delayForAttempt(4));
    }
}
