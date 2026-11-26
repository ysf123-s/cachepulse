package com.cachepulse.gateway;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.ReactiveDiscoveryClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/** 首页的微服务总览：实例数、各实例健康、路由熔断状态；结果缓存，打开首页的人再多也只查一次。 */
class SystemOverviewControllerTest {
    private final ReactiveDiscoveryClient discovery = mock(ReactiveDiscoveryClient.class);
    private final CircuitBreakerRegistry registry = CircuitBreakerRegistry.ofDefaults();
    private final AtomicInteger healthChecks = new AtomicInteger();

    @Test
    void listsEveryServiceWithItsInstancesHealthAndBreaker() {
        when(discovery.getInstances(anyString())).thenReturn(Flux.empty());
        when(discovery.getInstances("archive-service")).thenReturn(Flux.just(
                instance("archive-service", "10.0.0.2", 18092), instance("archive-service", "10.0.0.3", 28092)));
        when(discovery.getInstances("gateway")).thenReturn(Flux.just(instance("gateway", "10.0.0.1", 18090)));
        when(discovery.getInstances("delivery-service")).thenReturn(Flux.error(new IllegalStateException("nacos down")));
        registry.circuitBreaker("archive-service").transitionToOpenState();
        registry.circuitBreaker("demo-user-service");

        SystemOverviewController.Overview overview = controller().overview().block();

        assertThat(overview.services()).extracting(SystemOverviewController.Service::serviceId)
                .containsExactlyElementsOf(SystemOverviewController.SERVICES);
        var archive = service(overview, "archive-service");
        assertThat(archive.instances()).containsExactly(new SystemOverviewController.Instance(18092, "UP"),
                new SystemOverviewController.Instance(28092, "DOWN"));
        assertThat(archive.circuitBreaker().state()).isEqualTo("OPEN");
        var user = service(overview, "demo-user-service");
        assertThat(user.instances()).as("not registered").isEmpty();
        assertThat(user.circuitBreaker().state()).isEqualTo("CLOSED");
        assertThat(user.circuitBreaker().failureRate()).as("too few calls").isEqualTo(-1f);
        assertThat(service(overview, "gateway").circuitBreaker()).as("the gateway has no route breaker").isNull();
        assertThat(service(overview, "delivery-service").registryError()).isEqualTo("IllegalStateException");
    }

    @Test
    void cachesTheResultSoEveryPageViewDoesNotProbeEveryInstance() {
        when(discovery.getInstances(anyString())).thenReturn(Flux.empty());
        when(discovery.getInstances("gateway")).thenReturn(Flux.just(instance("gateway", "10.0.0.1", 18090)));
        SystemOverviewController controller = controller();

        var first = controller.overview().block();
        var second = controller.overview().block();

        assertThat(second).isSameAs(first);
        assertThat(healthChecks).hasValue(1);
    }

    private SystemOverviewController controller() {
        return new SystemOverviewController(discovery, instance -> {
            healthChecks.incrementAndGet();
            return Mono.just(instance.getHost().endsWith(".3") ? "DOWN" : "UP");
        }, () -> registry);
    }

    private static ServiceInstance instance(String service, String host, int port) {
        return new DefaultServiceInstance(service + "-" + port, service, host, port, false);
    }

    private static SystemOverviewController.Service service(SystemOverviewController.Overview overview, String id) {
        return overview.services().stream().filter(s -> s.serviceId().equals(id)).findFirst().orElseThrow();
    }
}
