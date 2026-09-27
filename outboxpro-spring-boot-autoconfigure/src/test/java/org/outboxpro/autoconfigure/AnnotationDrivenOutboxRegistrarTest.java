package org.outboxpro.autoconfigure;

import org.junit.jupiter.api.Test;
import org.outboxpro.core.annotation.OutboxEvent;
import org.outboxpro.core.annotation.OutboxHandler;
import org.outboxpro.core.annotation.RetryPolicySpec;
import org.outboxpro.core.context.EventContext;
import org.outboxpro.core.event.EventDefinition;
import org.outboxpro.core.event.EventRegistry;
import org.outboxpro.core.event.EventRoute;
import org.outboxpro.core.exception.EventConfigurationException;
import org.outboxpro.core.handler.AnnotatedOutboxHandler;
import org.outboxpro.core.handler.OutboxProHandler;
import org.outboxpro.core.subscription.OutboxProSubscription;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.annotation.DependsOn;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 注解装配的冲突、代理和混合声明回归测试。 */
class AnnotationDrivenOutboxRegistrarTest {
    @OutboxEvent(eventType = "created", exchange = "events")
    record Payload(int id) { }

    @OutboxHandler(event = Payload.class, queue = "queue", retry = @RetryPolicySpec(enabled = false))
    static class Handler extends AnnotatedOutboxHandler<Payload> {
        @Override public void handle(EventContext<Payload> context) { }
    }

    @OutboxHandler(event = Payload.class, queue = "queue", exchange = "other")
    static class OtherExchangeHandler extends AnnotatedOutboxHandler<Payload> {
        @Override public void handle(EventContext<Payload> context) { }
    }

    @OutboxEvent(eventType = "updated", exchange = "events")
    record UpdatedPayload(int id) { }

    @OutboxHandler(event = UpdatedPayload.class, queue = "queue")
    static class UpdatedHandler extends AnnotatedOutboxHandler<UpdatedPayload> {
        @Override public void handle(EventContext<UpdatedPayload> context) { }
    }

    @Test
    void sameGroupMergesBindingsAndPreservesSubscriptionIdentity() {
        var beans = new DefaultListableBeanFactory();
        new AnnotationDrivenOutboxRegistrar(new EventRegistry(), List.of(new Handler(), new UpdatedHandler()), beans);
        assertThat(beans.getBeansOfType(OutboxProSubscription.class).values()).singleElement().satisfies(subscription -> {
            assertThat(subscription.getName()).isEqualTo("annotated-queue");
            assertThat(subscription.getBindings()).extracting(binding -> binding.eventType())
                    .containsExactlyInAnyOrder("created", "updated");
        });
    }

    @Test
    void duplicateSubscriptionNamesFailInsteadOfDroppingBindings() {
        var beans = new DefaultListableBeanFactory();
        assertThatThrownBy(() -> new AnnotationDrivenOutboxRegistrar(new EventRegistry(),
                List.of(new Handler(), new OtherExchangeHandler()), beans))
                .isInstanceOf(EventConfigurationException.class).hasMessageContaining("subscription");
        assertThat(beans.getBeansOfType(OutboxProSubscription.class)).isEmpty();
    }

    @Test
    void springJdkAndCglibProxiesRetainAnnotationsAndRetryOverride() {
        for (boolean cglib : List.of(false, true)) {
            ProxyFactory factory = new ProxyFactory(new Handler());
            factory.setProxyTargetClass(cglib);
            var proxy = (OutboxProHandler<?>) factory.getProxy();
            var beans = new DefaultListableBeanFactory();
            var registry = new EventRegistry();
            new AnnotationDrivenOutboxRegistrar(registry, List.of(proxy), beans);
            assertThat(registry.find("created")).isNotNull();
            assertThat(beans.getBeansOfType(OutboxProSubscription.class).values()).singleElement()
                    .satisfies(subscription -> assertThat(subscription.getBindings()).singleElement()
                            .satisfies(binding -> assertThat(binding.retryPolicy().enabled()).isFalse()));
        }
    }

    @Test
    void matchingBuilderDefinitionCoexists() {
        var registry = new EventRegistry();
        var definition = new EventDefinition<>("created", "v1", Payload.class, new EventRoute("events", "created"));
        registry.register(definition);
        new AnnotationDrivenOutboxRegistrar(registry, List.of(new Handler()), new DefaultListableBeanFactory());
        assertThat(registry.require("created")).isSameAs(definition);
    }

    @Test
    void incompatibleBuilderDefinitionFailsFast() {
        for (EventDefinition<?> definition : List.of(
                new EventDefinition<>("created", "v1", String.class, new EventRoute("events", "created")),
                new EventDefinition<>("created", "v1", Payload.class, new EventRoute("wrong", "created")),
                new EventDefinition<>("created", "v1", Payload.class, new EventRoute("events", "wrong")),
                new EventDefinition<>("created", "v2", Payload.class, new EventRoute("events", "created")))) {
            var registry = new EventRegistry();
            registry.register(definition);
            assertThatThrownBy(() -> new AnnotationDrivenOutboxRegistrar(registry, List.of(new Handler()),
                    new DefaultListableBeanFactory())).isInstanceOf(EventConfigurationException.class);
        }
    }

    @Test
    void registrarExplicitlyDependsOnBuilderInitialization() {
        var method = java.util.Arrays.stream(OutboxProAutoConfiguration.class.getDeclaredMethods())
                .filter(m -> m.getName().equals("outboxProAnnotationDrivenRegistrar")).findFirst().orElseThrow();
        assertThat(method.getAnnotation(DependsOn.class)).isNotNull();
        assertThat(method.getAnnotation(DependsOn.class).value()).contains("outboxProEventRegistryInitializer");
    }
}
