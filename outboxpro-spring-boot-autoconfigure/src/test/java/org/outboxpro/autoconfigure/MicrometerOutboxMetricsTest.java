package org.outboxpro.autoconfigure;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;

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

    /** V1.4 延迟与周期指标：Timer 必须带低基数标签，空闲/有活轮可区分。 */
    @Test
    void durationAndRelayCycleMetricsShouldBeRecorded() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerOutboxMetrics metrics = new MicrometerOutboxMetrics(registry);

        metrics.publishDuration("it.event", "producer-a", Duration.ofMillis(25));
        metrics.consumeDuration("it.event", "consumer-a", "queue-a", Duration.ofMillis(10));
        metrics.relayCycle(Duration.ofMillis(5), true);
        metrics.relayCycle(Duration.ofMillis(2), false);
        metrics.publishConfirmTimeout("it.event", "producer-a");

        assertThat(registry.get("outboxpro.publish.duration")
                .tags("event_type", "it.event", "producer", "producer-a")
                .timer().count()).isEqualTo(1);
        assertThat(registry.get("outboxpro.consume.duration")
                .tags("event_type", "it.event", "consumer", "consumer-a", "queue", "queue-a")
                .timer().count()).isEqualTo(1);
        assertThat(registry.get("outboxpro.relay.cycle.duration").tag("claimed", "true")
                .timer().count()).isEqualTo(1);
        assertThat(registry.get("outboxpro.relay.cycle.duration").tag("claimed", "false")
                .timer().count()).isEqualTo(1);
        assertThat(registry.get("outboxpro.publish.confirm.timeout")
                .tags("event_type", "it.event", "producer", "producer-a")
                .counter().count()).isEqualTo(1.0);
    }
}
