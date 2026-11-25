package com.cachepulse.cache.starter;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * 查库并发上限。名额用完时最多等待 acquireTimeoutMs，仍拿不到就拒绝（HTTP 503），
 * 不让请求在数据库连接池前排队：缓存大面积失效、心跳降级或 Redis 熔断时，请求会集中查库。
 *
 * <p>只限制真正查库的请求，同 key 合并等待的请求不占名额。布隆过滤器判定一定不存在的主键走
 * {@link Lane#SUSPECT} 窄通道，被它占满不影响正常查询。</p>
 */
public class CacheLoadGuard {
    public enum Lane { NORMAL, SUSPECT }

    public record LaneStatus(int limit, int inUse, long acquireTimeoutMs, long rejected) {
    }

    private record Limiter(Semaphore permits, int limit, long timeoutMs, AtomicLong rejected) {
    }

    private final Map<Lane, Limiter> lanes = new EnumMap<>(Lane.class);

    public CacheLoadGuard(int normalLimit, long normalTimeoutMs, int suspectLimit, long suspectTimeoutMs) {
        if (normalLimit < 1 || suspectLimit < 1 || normalTimeoutMs < 0 || suspectTimeoutMs < 0) {
            throw new IllegalArgumentException("Cache load limits must be >= 1 and timeouts >= 0");
        }
        lanes.put(Lane.NORMAL, new Limiter(new Semaphore(normalLimit), normalLimit, normalTimeoutMs, new AtomicLong()));
        lanes.put(Lane.SUSPECT, new Limiter(new Semaphore(suspectLimit), suspectLimit, suspectTimeoutMs, new AtomicLong()));
    }

    public static CacheLoadGuard unlimited() {
        return new CacheLoadGuard(Integer.MAX_VALUE, 0, Integer.MAX_VALUE, 0);
    }

    public <T> T run(Lane lane, Supplier<T> load) {
        Limiter limiter = lanes.get(lane);
        boolean acquired;
        try {
            acquired = limiter.permits().tryAcquire(limiter.timeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            acquired = false;
        }
        if (!acquired) {
            limiter.rejected().incrementAndGet();
            throw new CacheLoadRejectedException(lane, limiter.limit());
        }
        try {
            return load.get();
        } finally {
            limiter.permits().release();
        }
    }

    public long rejected(Lane lane) {
        return lanes.get(lane).rejected().get();
    }

    public int inUse(Lane lane) {
        Limiter limiter = lanes.get(lane);
        return limiter.limit() - limiter.permits().availablePermits();
    }

    public Map<Lane, LaneStatus> status() {
        Map<Lane, LaneStatus> status = new EnumMap<>(Lane.class);
        lanes.forEach((lane, limiter) -> status.put(lane,
                new LaneStatus(limiter.limit(), inUse(lane), limiter.timeoutMs(), limiter.rejected().get())));
        return status;
    }
}
