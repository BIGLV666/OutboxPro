package org.outboxpro.autoconfigure;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 死信配置约束测试。 */
class DeadLetterConfigurationValidatorTest {
    @Test
    void replayEnabledWithoutLedgerMustFailAtStartupValidation() {
        OutboxProProperties properties = new OutboxProProperties();
        properties.getDlq().getLedger().setEnabled(false);
        properties.getDlq().getReplay().setEnabled(true);

        assertThrows(IllegalStateException.class, () -> new DeadLetterConfigurationValidator(properties));
    }

    @Test
    void validLedgerAndReplayConfigurationMustPass() {
        OutboxProProperties properties = new OutboxProProperties();
        properties.getDlq().getLedger().setEnabled(true);
        properties.getDlq().getReplay().setEnabled(true);

        assertDoesNotThrow(() -> new DeadLetterConfigurationValidator(properties));
    }
}
