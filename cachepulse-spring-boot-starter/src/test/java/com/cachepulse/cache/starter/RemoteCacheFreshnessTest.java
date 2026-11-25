package com.cachepulse.cache.starter;

import com.cachepulse.cache.core.CacheKeys;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RemoteCacheFreshnessTest {
    private final AtomicLong now = new AtomicLong(1_000_000);
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> values = mock(ValueOperations.class);
    private RemoteCacheFreshness freshness;

    @BeforeEach
    void setUp() {
        when(redis.opsForValue()).thenReturn(values);
        freshness = new RemoteCacheFreshness(redis, new ObjectMapper(), now::get);
    }

    @Test
    void unusableUntilTheServiceHasRegisteredItsGroups() {
        signal("FRESH", 5);

        assertThat(freshness.requiredVersion()).isZero();
        assertThat(freshness.cacheUsable()).isFalse();
    }

    @Test
    void unusableWhenTheSignalIsMissing() {
        freshness.requireRegistryVersion(3);
        when(values.get(CacheKeys.FRESHNESS)).thenReturn(null);

        assertThat(freshness.cacheUsable()).isFalse();
    }

    @Test
    void unusableWhenTheStateIsNotFresh() {
        freshness.requireRegistryVersion(3);
        signal("STALE", 3);

        assertThat(freshness.cacheUsable()).isFalse();
    }

    @Test
    void unusableUntilCacheSyncHasLoadedTheRequiredRegistryVersion() {
        freshness.requireRegistryVersion(3);
        signal("FRESH", 2);

        assertThat(freshness.cacheUsable()).isFalse();
    }

    @Test
    void usableWhenFreshAndRegistryVersionReachesTheRequiredOne() {
        freshness.requireRegistryVersion(3);
        signal("FRESH", 3);
        assertThat(freshness.cacheUsable()).isTrue();

        now.addAndGet(RemoteCacheFreshness.LOCAL_CACHE_MS);
        signal("FRESH", 4);
        assertThat(freshness.cacheUsable()).isTrue();
    }

    @Test
    void unreadableSignalOrRedisFailureBypassesTheCache() {
        freshness.requireRegistryVersion(1);
        when(values.get(CacheKeys.FRESHNESS)).thenReturn("not json");
        assertThat(freshness.cacheUsable()).isFalse();

        now.addAndGet(RemoteCacheFreshness.LOCAL_CACHE_MS);
        when(values.get(CacheKeys.FRESHNESS)).thenThrow(new RedisConnectionFailureException("redis offline"));
        assertThat(freshness.cacheUsable()).isFalse();
    }

    @Test
    void resultIsCachedLocallyFor200Milliseconds() {
        freshness.requireRegistryVersion(1);
        signal("FRESH", 1);
        assertThat(freshness.cacheUsable()).isTrue();

        signal("STALE", 1);
        now.addAndGet(RemoteCacheFreshness.LOCAL_CACHE_MS - 1);
        assertThat(freshness.cacheUsable()).isTrue();
        verify(values, times(1)).get(CacheKeys.FRESHNESS);

        now.addAndGet(1);
        assertThat(freshness.cacheUsable()).isFalse();
        verify(values, times(2)).get(CacheKeys.FRESHNESS);
    }

    @Test
    void changingTheRequiredVersionDropsTheLocalCache() {
        freshness.requireRegistryVersion(1);
        signal("FRESH", 1);
        assertThat(freshness.cacheUsable()).isTrue();

        freshness.requireRegistryVersion(2);

        assertThat(freshness.cacheUsable()).isFalse();
        verify(values, times(2)).get(CacheKeys.FRESHNESS);
    }

    private void signal(String state, long registryVersion) {
        when(values.get(CacheKeys.FRESHNESS)).thenReturn(
                "{\"state\":\"" + state + "\",\"registryVersion\":" + registryVersion + ",\"instance\":\"sync-1\"}");
    }
}
