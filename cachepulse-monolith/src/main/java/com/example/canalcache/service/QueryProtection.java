package com.example.canalcache.service;

import com.example.canalcache.service.CacheLoadGuard.Lane;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 查询侧保护：Redis 出错或熔断时直接查库、查库并发上限、按主键布隆过滤器分流。
 * 只影响查询读写 Redis 的方式，不改变删缓存的处理；一致性仍由 CDC 删除和 load_token 保证。
 */
@Component
public class QueryProtection {
    public record Status(RedisCircuitBreaker.Status redis, Map<Lane, CacheLoadGuard.LaneStatus> loads,
                         Map<String, PrimaryKeyFilters.FilterStatus> bloomFilters) {
    }

    /** null 表示不降级：Redis 出错直接抛出。 */
    private final RedisCircuitBreaker breaker;
    private final CacheLoadGuard guard;
    private final PrimaryKeyFilters primaryKeys;

    @Autowired
    public QueryProtection(@Value("${cache.redis-breaker.failure-threshold:5}") int failureThreshold,
                           @Value("${cache.redis-breaker.open-ms:5000}") long openMs,
                           @Value("${cache.load.max-concurrency:6}") int maxConcurrency,
                           @Value("${cache.load.acquire-timeout-ms:200}") long acquireTimeoutMs,
                           @Value("${cache.load.suspect-max-concurrency:2}") int suspectMaxConcurrency,
                           @Value("${cache.load.suspect-acquire-timeout-ms:0}") long suspectAcquireTimeoutMs,
                           PrimaryKeyFilters primaryKeys, MeterRegistry registry) {
        this(new RedisCircuitBreaker(failureThreshold, openMs, System::currentTimeMillis),
                new CacheLoadGuard(maxConcurrency, acquireTimeoutMs, suspectMaxConcurrency, suspectAcquireTimeoutMs),
                primaryKeys);
        Gauge.builder("cache.redis.breaker.open", breaker,
                        b -> b.status().state() == RedisCircuitBreaker.State.OPEN ? 1 : 0)
                .description("1 while queries skip Redis after consecutive failures").register(registry);
        FunctionCounter.builder("cache.query.redis.fallbacks", breaker, b -> b.status().fallbacks())
                .description("Queries that read MySQL directly because Redis failed or the breaker was open")
                .register(registry);
        for (Lane lane : Lane.values()) {
            Gauge.builder("cache.load.in_use", guard, g -> g.inUse(lane)).tag("lane", lane.name())
                    .description("Database loads currently running").register(registry);
            FunctionCounter.builder("cache.load.rejected", guard, g -> g.rejected(lane)).tag("lane", lane.name())
                    .description("Queries rejected with 503 because the database load limit was reached")
                    .register(registry);
        }
    }

    public QueryProtection(RedisCircuitBreaker breaker, CacheLoadGuard guard, PrimaryKeyFilters primaryKeys) {
        this.breaker = breaker;
        this.guard = guard;
        this.primaryKeys = primaryKeys;
    }

    /** 不降级、不限流、不分流，保持原有行为。 */
    public static QueryProtection disabled() {
        return new QueryProtection(null, CacheLoadGuard.unlimited(), PrimaryKeyFilters.none());
    }

    /** 本次查询能否访问 Redis；熔断期间返回 false，调用方直接查库。 */
    boolean redisAllowed() {
        if (breaker == null || breaker.allowRequest()) {
            return true;
        }
        breaker.recordSkipped();
        return false;
    }

    void redisSucceeded() {
        if (breaker != null) {
            breaker.recordSuccess();
        }
    }

    /** Redis 出错：有熔断器时记录并让调用方改为直接查库；没有时原样抛出。 */
    void redisFailed(DataAccessException error) {
        if (breaker == null) {
            throw error;
        }
        breaker.recordFailure(error);
    }

    /** 按布隆过滤器把待查库的主键分成两组，正常组在前；空组不返回。 */
    Map<Lane, List<Long>> lanes(String table, List<Long> ids) {
        Map<Lane, List<Long>> lanes = new EnumMap<>(Lane.class);
        for (Long id : ids) {
            Lane lane = primaryKeys.mightContain(table, id) ? Lane.NORMAL : Lane.SUSPECT;
            lanes.computeIfAbsent(lane, ignored -> new ArrayList<>()).add(id);
        }
        return lanes;
    }

    <T> T load(Lane lane, Supplier<T> loader) {
        return guard.run(lane, loader);
    }

    public Status status() {
        return new Status(breaker == null ? null : breaker.status(), guard.status(), primaryKeys.status());
    }
}
