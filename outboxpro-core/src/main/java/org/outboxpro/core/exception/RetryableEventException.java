package org.outboxpro.core.exception;

/**
 * 表示当前事件可以稍后重新执行的业务异常。
 */
public class RetryableEventException extends RuntimeException {
/**
 * 执行该公共 API 定义的操作。
 */
    public RetryableEventException(String message) { super(message); }
/**
 * 执行该公共 API 定义的操作。
 */
    public RetryableEventException(String message, Throwable cause) { super(message, cause); }
}



