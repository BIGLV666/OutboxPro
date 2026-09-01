package org.outboxpro.core.exception;

import org.outboxpro.core.annotation.NonRetryable;

/**
 * 不可重试异常的统一判定入口。
 *
 * <p>判定规则：异常本身或其因果链中任一异常，满足以下任一条件即为不可重试——</p>
 * <ul>
 *   <li>是 {@link NonRetryableEventException} 或其子类；</li>
 *   <li>异常类（或其任意父类）标注了 {@link NonRetryable}。</li>
 * </ul>
 *
 * <p>沿因果链向上查找是为了兼容包装语义：Handler 中被捕获后重新包装的受检异常
 * 仍然保留原有的不可重试声明。因果链遍历有深度上限，防止循环引用造成死循环。</p>
 */
public final class NonRetryableExceptions {

    /** 因果链最大遍历深度；正常业务包装链远小于该值。 */
    private static final int MAX_CAUSE_DEPTH = 10;

    private NonRetryableExceptions() {
    }

    /**
     * 判断异常因果链中是否存在不可重试异常。
     *
     * @param error 待判定异常，可为 {@code null}
     * @return {@code true} 表示不可重试，框架将跳过重试直接进入死信流程
     */
    public static boolean isNonRetryable(Throwable error) {
        Throwable current = error;
        int depth = 0;
        while (current != null && depth++ < MAX_CAUSE_DEPTH) {
            if (current instanceof NonRetryableEventException || isAnnotated(current.getClass())) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /** 沿父类层次检查异常类是否标注了 {@link NonRetryable}。 */
    private static boolean isAnnotated(Class<?> type) {
        for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
            if (current.isAnnotationPresent(NonRetryable.class)) {
                return true;
            }
        }
        return false;
    }
}
