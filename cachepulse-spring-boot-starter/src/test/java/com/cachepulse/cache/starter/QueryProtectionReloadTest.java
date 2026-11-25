package com.cachepulse.cache.starter;

import com.cachepulse.cache.starter.CacheLoadGuard.Lane;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.context.environment.EnvironmentChangeEvent;
import org.springframework.mock.env.MockEnvironment;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 查询保护参数随 Nacos 推送的配置变化重建（设计文档第 6.8 节）。 */
class QueryProtectionReloadTest {
    private final MockEnvironment environment = new MockEnvironment();
    private QueryProtection protection;

    @BeforeEach
    void setUp() {
        protection = new QueryProtection(5, 5_000, 1, 0, 2, 0, PrimaryKeyFilters.none(),
                new SimpleMeterRegistry(), environment);
    }

    @Test
    void cacheLoadKeysRebuildTheLoadLimits() {
        environment.setProperty("cache.load.max-concurrency", "3");
        environment.setProperty("cache.load.acquire-timeout-ms", "50");
        environment.setProperty("cache.load.suspect-max-concurrency", "4");

        protection.onEnvironmentChange(new EnvironmentChangeEvent(Set.of("cache.load.max-concurrency",
                "cache.load.acquire-timeout-ms", "cache.load.suspect-max-concurrency")));

        assertThat(protection.status().loads().get(Lane.NORMAL).limit()).isEqualTo(3);
        assertThat(protection.status().loads().get(Lane.NORMAL).acquireTimeoutMs()).isEqualTo(50);
        assertThat(protection.status().loads().get(Lane.SUSPECT).limit()).isEqualTo(4);
    }

    @Test
    void unrelatedKeysKeepTheCurrentLimits() {
        environment.setProperty("cache.load.max-concurrency", "3");

        protection.onEnvironmentChange(new EnvironmentChangeEvent(Set.of("spring.application.name")));

        assertThat(protection.status().loads().get(Lane.NORMAL).limit()).isEqualTo(1);
    }

    @Test
    void loadsRunningDuringTheReloadReturnTheirPermitToTheOldGuard() {
        environment.setProperty("cache.load.max-concurrency", "2");

        String result = protection.load(Lane.NORMAL, () -> {
            assertThatThrownBy(() -> protection.load(Lane.NORMAL, () -> "rejected by the old limit of 1"))
                    .isInstanceOf(CacheLoadRejectedException.class);
            protection.onEnvironmentChange(new EnvironmentChangeEvent(Set.of("cache.load.max-concurrency")));
            assertThat(protection.status().loads().get(Lane.NORMAL).inUse()).isZero();
            return protection.load(Lane.NORMAL, () -> "new limit applies");
        });

        assertThat(result).isEqualTo("new limit applies");
        assertThat(protection.status().loads().get(Lane.NORMAL).inUse()).isZero();
        assertThat(protection.status().loads().get(Lane.NORMAL).limit()).isEqualTo(2);
    }

    @Test
    void breakerKeysRebuildTheBreaker() {
        protection.redisFailed(new org.springframework.data.redis.RedisConnectionFailureException("redis offline"));
        assertThat(protection.status().redis().consecutiveFailures()).isEqualTo(1);
        environment.setProperty("cache.redis-breaker.failure-threshold", "1");

        protection.onEnvironmentChange(new EnvironmentChangeEvent(Set.of("cache.redis-breaker.failure-threshold")));

        assertThat(protection.status().redis().consecutiveFailures()).isZero();
        protection.redisFailed(new org.springframework.data.redis.RedisConnectionFailureException("redis offline"));
        assertThat(protection.status().redis().state()).isEqualTo(RedisCircuitBreaker.State.OPEN);
    }
}
