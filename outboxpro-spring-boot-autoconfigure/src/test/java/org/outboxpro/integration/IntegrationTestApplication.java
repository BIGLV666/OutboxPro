package org.outboxpro.integration;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 集成测试引导应用。
 * 仅作为自动装配入口存在；测试专属 Bean 由各测试类以内部 {@code @Configuration} 提供。
 */
@SpringBootApplication
public class IntegrationTestApplication {
}
