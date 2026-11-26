package com.cachepulse.gateway;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.circuitbreaker.resilience4j.ReactiveResilience4JCircuitBreakerFactory;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.ReactiveDiscoveryClient;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 微服务总览（首页 index.html 用）：每个服务在 Nacos 里注册了几个实例、各实例 /actuator/health 的结果，
 * 以及网关上这个服务路由的熔断状态。只读、公开（GET /api/**），不返回实例 IP。
 *
 * <p>结果缓存 3 秒：每次刷新要对所有实例各发一次健康检查，缓存后多少人同时打开首页都只查一次。
 * 单个实例 1 秒内没回应就记为 DOWN，Nacos 读不到时该服务记为没有实例。</p>
 */
@RestController
public class SystemOverviewController {
    /** 页面按这个顺序画卡片；名字和 Nacos 里的服务名、网关路由的熔断名一致。 */
    static final List<String> SERVICES = List.of("gateway", "cache-sync-service", "archive-service",
            "delivery-service", "demo-user-service", "demo-inventory-service");
    static final Duration CACHE_FOR = Duration.ofSeconds(3);
    static final Duration HEALTH_TIMEOUT = Duration.ofSeconds(1);
    private static final ParameterizedTypeReference<Map<String, Object>> JSON = new ParameterizedTypeReference<>() { };

    private final ReactiveDiscoveryClient discovery;
    private final Function<ServiceInstance, Mono<String>> health;
    private final Supplier<CircuitBreakerRegistry> breakers;
    private final Mono<Overview> cached;

    @Autowired
    public SystemOverviewController(ReactiveDiscoveryClient discovery, WebClient.Builder web,
                                    ObjectProvider<ReactiveResilience4JCircuitBreakerFactory> breakerFactory) {
        this(discovery, httpHealth(web.build()), () -> {
            ReactiveResilience4JCircuitBreakerFactory factory = breakerFactory.getIfAvailable();
            return factory == null ? null : factory.getCircuitBreakerRegistry();
        });
    }

    SystemOverviewController(ReactiveDiscoveryClient discovery, Function<ServiceInstance, Mono<String>> health,
                             Supplier<CircuitBreakerRegistry> breakers) {
        this.discovery = discovery;
        this.health = health;
        this.breakers = breakers;
        this.cached = Mono.defer(this::collect).cache(CACHE_FOR);
    }

    public record Overview(Instant generatedAt, List<Service> services) { }

    /** registryError 不为 null 时表示这次没从 Nacos 读到这个服务。 */
    public record Service(String serviceId, List<Instance> instances, Breaker circuitBreaker, String registryError) { }

    public record Instance(int port, String status) { }

    /** failureRate、slowCallRate 为 -1 表示调用次数还不够，Resilience4j 不统计。 */
    public record Breaker(String state, float failureRate, float slowCallRate, int bufferedCalls) { }

    @GetMapping("/api/system/overview")
    public Mono<Overview> overview() {
        return cached;
    }

    private Mono<Overview> collect() {
        CircuitBreakerRegistry registry = breakers.get();
        return Flux.fromIterable(SERVICES)
                .concatMap(id -> discovery.getInstances(id)
                        .flatMap(instance -> health.apply(instance).map(status -> new Instance(instance.getPort(), status)))
                        .sort(Comparator.comparingInt(Instance::port).thenComparing(Instance::status))
                        .collectList()
                        .map(instances -> new Service(id, instances, breaker(registry, id), null))
                        .onErrorResume(e -> Mono.just(new Service(id, List.of(), breaker(registry, id),
                                e.getClass().getSimpleName()))))
                .collectList()
                .map(services -> new Overview(Instant.now(), services));
    }

    private static Breaker breaker(CircuitBreakerRegistry registry, String name) {
        if (registry == null) {
            return null;
        }
        return registry.find(name).map(SystemOverviewController::breaker).orElse(null);
    }

    private static Breaker breaker(CircuitBreaker breaker) {
        CircuitBreaker.Metrics metrics = breaker.getMetrics();
        return new Breaker(breaker.getState().name(), metrics.getFailureRate(), metrics.getSlowCallRate(),
                metrics.getNumberOfBufferedCalls());
    }

    static Function<ServiceInstance, Mono<String>> httpHealth(WebClient web) {
        return instance -> web.get().uri(instance.getUri().resolve("/actuator/health"))
                .exchangeToMono(response -> response.bodyToMono(JSON)
                        .map(body -> String.valueOf(body.getOrDefault("status", "UNKNOWN")))
                        .defaultIfEmpty(response.statusCode().is2xxSuccessful() ? "UP" : "DOWN"))
                .timeout(HEALTH_TIMEOUT)
                .onErrorReturn("DOWN");
    }
}
