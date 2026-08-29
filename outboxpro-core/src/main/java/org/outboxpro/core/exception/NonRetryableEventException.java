package org.outboxpro.core.exception;

/**
 * 表示当前事件不应再次执行的业务异常。
 */
public class NonRetryableEventException extends RuntimeException {
/**
 * 执行该公共 API 定义的操作。
 */
    public NonRetryableEventException(String message) { super(message); }
/**
 * 执行该公共 API 定义的操作。
 */
    public NonRetryableEventException(String message, Throwable cause) { super(message, cause); }
}



