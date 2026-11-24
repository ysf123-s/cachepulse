package com.example.canalcache.service;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * 查询侧的 Redis 熔断。连续失败达到阈值后熔断 openMs：期间查询不访问 Redis，直接查库；
 * 期满只放一个请求试探，成功即恢复，失败再熔断一个周期。试探请求没有回音时，下一个周期再放一个。
 *
 * <p>只用于查询。删缓存失败仍整批回滚重试，不能跳过。</p>
 */
@Slf4j
public class RedisCircuitBreaker {
    public enum State { CLOSED, OPEN }

    /** fallbacks：因 Redis 出错或熔断而改为直接查库的查询次数，本进程累计。 */
    public record Status(State state, int consecutiveFailures, int failureThreshold, long openMs,
                         long openRemainingMs, long fallbacks, String lastError) {
    }

    private final int failureThreshold;
    private final long openMs;
    private final LongSupplier clock;
    private final AtomicInteger failures = new AtomicInteger();
    /** 0 表示闭合；否则是熔断到期时间。到期后第一个把它推后一个周期的请求负责试探。 */
    private final AtomicLong openUntil = new AtomicLong();
    private final AtomicLong fallbacks = new AtomicLong();
    private volatile String lastError;

    public RedisCircuitBreaker(int failureThreshold, long openMs, LongSupplier clock) {
        if (failureThreshold < 1 || openMs < 1) {
            throw new IllegalArgumentException("Redis breaker needs failure-threshold >= 1 and open-ms >= 1");
        }
        this.failureThreshold = failureThreshold;
        this.openMs = openMs;
        this.clock = clock;
    }

    /** 本次查询能否访问 Redis。返回 false 时调用方直接查库，并调用 {@link #recordSkipped()}。 */
    public boolean allowRequest() {
        long until = openUntil.get();
        if (until == 0) {
            return true;
        }
        long now = clock.getAsLong();
        return now >= until && openUntil.compareAndSet(until, now + openMs);
    }

    public void recordSuccess() {
        failures.set(0);
        if (openUntil.getAndSet(0) != 0) {
            log.info("Redis reachable again, queries use the cache");
        }
    }

    /** Redis 出错，本次查询已改为直接查库。 */
    public void recordFailure(RuntimeException error) {
        fallbacks.incrementAndGet();
        lastError = error.getClass().getSimpleName() + ": " + error.getMessage();
        int count = failures.incrementAndGet();
        if (count >= failureThreshold && openUntil.getAndSet(clock.getAsLong() + openMs) == 0) {
            log.warn("Redis failed {} times in a row, queries read MySQL directly for {} ms: {}",
                    count, openMs, lastError);
        }
    }

    public void recordSkipped() {
        fallbacks.incrementAndGet();
    }

    public Status status() {
        long until = openUntil.get();
        return new Status(until == 0 ? State.CLOSED : State.OPEN, failures.get(), failureThreshold, openMs,
                until == 0 ? 0 : Math.max(0, until - clock.getAsLong()), fallbacks.get(), lastError);
    }
}
