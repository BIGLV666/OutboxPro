package org.outboxpro.autoconfigure;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** 累计死信原因 Counter：独立指标名，标签值收敛到稳定枚举。 */
class MicrometerOutboxMetricsTest {

    @Test
    void deadReasonCounterShouldCarryStableReasonTag() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerOutboxMetrics metrics = new MicrometerOutboxMetrics(registry);

        metrics.consumeDead("it.event", "consumer-a", "queue-a", "RETRY_EXHAUSTED");
        metrics.consumeDead("it.event", "consumer-a", "queue-a", "NOT_A_REASON");

        assertThat(registry.get("outboxpro.consume.dead")
                .tags("event_type", "it.event", "consumer", "consumer-a", "queue", "queue-a")
                .counter().count()).isEqualTo(2.0);
        assertThat(registry.get("outboxpro.consume.dead.reason")
                .tags("event_type", "it.event", "consumer", "consumer-a",
                        "queue", "queue-a", "reason", "RETRY_EXHAUSTED")
                .counter().count()).isEqualTo(1.0);
        assertThat(registry.get("outboxpro.consume.dead.reason")
                .tags("event_type", "it.event", "consumer", "consumer-a",
                        "queue", "queue-a", "reason", "unknown")
                .counter().count()).isEqualTo(1.0);
    }
}
