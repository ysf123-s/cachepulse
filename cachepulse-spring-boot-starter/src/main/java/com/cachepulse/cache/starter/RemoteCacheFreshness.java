package com.cachepulse.cache.starter;

import com.cachepulse.cache.core.CacheFreshness;
import com.cachepulse.cache.core.FreshnessSignal;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.function.LongSupplier;

/**
 * 读取同步服务写入的新鲜度信号，本地缓存 200ms。以下情况都绕过缓存直接查库：
 * 本服务的分组还没注册成功；信号不存在（同步服务停了或 Redis 不可用）；状态不是 FRESH；
 * 同步服务还没加载到本服务注册时的注册表版本（新分组的变更它还不会删缓存）。
 */
public class RemoteCacheFreshness implements CacheFreshness {
    static final long LOCAL_CACHE_MS = 200;

    private final StringRedisTemplate redis;
    private final ObjectMapper json;
    private final LongSupplier clock;
    private volatile long requiredVersion;
    private volatile boolean lastUsable;
    private volatile long lastCheckedAt = Long.MIN_VALUE;

    public RemoteCacheFreshness(StringRedisTemplate redis, ObjectMapper json) {
        this(redis, json, System::currentTimeMillis);
    }

    RemoteCacheFreshness(StringRedisTemplate redis, ObjectMapper json, LongSupplier clock) {
        this.redis = redis;
        this.json = json;
        this.clock = clock;
    }

    /** 注册成功后设置；0 表示还没注册，缓存不可用。 */
    public void requireRegistryVersion(long version) {
        requiredVersion = version;
        lastCheckedAt = Long.MIN_VALUE;
    }

    public long requiredVersion() {
        return requiredVersion;
    }

    @Override
    public boolean cacheUsable() {
        long now = clock.getAsLong();
        if (lastCheckedAt != Long.MIN_VALUE && now - lastCheckedAt < LOCAL_CACHE_MS) {
            return lastUsable;
        }
        boolean usable;
        try {
            FreshnessSignal signal = FreshnessSignal.read(redis, json);
            usable = requiredVersion > 0 && signal != null && signal.fresh() && signal.registryVersion() >= requiredVersion;
        } catch (RuntimeException e) {
            usable = false;
        }
        lastUsable = usable;
        lastCheckedAt = now;
        return usable;
    }
}
