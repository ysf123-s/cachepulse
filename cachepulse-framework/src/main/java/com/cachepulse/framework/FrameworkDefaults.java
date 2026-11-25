package com.cachepulse.framework;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 微服务基础设施的默认配置，优先级最低，服务配置和 Nacos 都能覆盖。取值来自 2026-11-24 的冒烟验证：
 * 超时只在 Feign 上设，关闭 Spring Cloud CircuitBreaker 默认 1 秒的 TimeLimiter 和线程池；
 * 关闭 LoadBalancer 的实例缓存（Nacos 客户端已有推送更新的缓存，否则下线后还会往旧实例发约 23 秒请求）。
 */
public class FrameworkDefaults implements EnvironmentPostProcessor {
    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        Map<String, Object> defaults = new LinkedHashMap<>();
        defaults.put("spring.cloud.loadbalancer.cache.enabled", "false");
        defaults.put("spring.cloud.openfeign.circuitbreaker.enabled", "true");
        defaults.put("spring.cloud.openfeign.micrometer.enabled", "true");
        defaults.put("spring.cloud.openfeign.client.config.default.connect-timeout", "1000");
        defaults.put("spring.cloud.openfeign.client.config.default.read-timeout", "3000");
        defaults.put("spring.cloud.circuitbreaker.resilience4j.disable-time-limiter", "true");
        defaults.put("spring.cloud.circuitbreaker.resilience4j.disable-thread-pool", "true");
        defaults.put("resilience4j.circuitbreaker.configs.default.sliding-window-type", "COUNT_BASED");
        defaults.put("resilience4j.circuitbreaker.configs.default.sliding-window-size", "20");
        defaults.put("resilience4j.circuitbreaker.configs.default.minimum-number-of-calls", "10");
        defaults.put("resilience4j.circuitbreaker.configs.default.failure-rate-threshold", "50");
        defaults.put("resilience4j.circuitbreaker.configs.default.slow-call-duration-threshold", "2s");
        defaults.put("resilience4j.circuitbreaker.configs.default.slow-call-rate-threshold", "80");
        defaults.put("resilience4j.circuitbreaker.configs.default.wait-duration-in-open-state", "10s");
        defaults.put("resilience4j.circuitbreaker.configs.default.permitted-number-of-calls-in-half-open-state", "3");
        defaults.put("management.tracing.sampling.probability", "1.0");
        defaults.put("management.endpoints.web.exposure.include", "health,info,metrics,prometheus");
        defaults.put("management.endpoint.health.probes.enabled", "true");
        defaults.put("management.metrics.tags.application", "${spring.application.name}");
        defaults.put("server.shutdown", "graceful");
        defaults.put("spring.lifecycle.timeout-per-shutdown-phase", "20s");
        defaults.put("spring.mvc.problemdetails.enabled", "true");
        environment.getPropertySources().addLast(new MapPropertySource("cachepulseFrameworkDefaults", defaults));
    }
}
