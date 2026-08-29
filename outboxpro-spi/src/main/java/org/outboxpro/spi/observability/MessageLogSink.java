package org.outboxpro.spi.observability;

import org.outboxpro.spi.observability.MessageTraceRecord;

/** 消息日志 Sink。Sink 失败不得传播到消息主流程。 */
/**
 * 消息生命周期日志扩展点，Sink 失败不得影响主消息流程。
 */
public interface MessageLogSink {
    void append(MessageTraceRecord record);
}



