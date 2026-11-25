package com.cachepulse.sync;

import com.cachepulse.cache.core.CacheKeys;
import com.cachepulse.cache.core.FreshnessSignal;
import com.cachepulse.cache.core.GroupRegistry;
import com.cachepulse.sync.CdcHeartbeatMonitor.State;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class FreshnessPublisherTest {
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final ValueOperations<String, String> values = mock(ValueOperations.class);
    private final CdcHeartbeatMonitor heartbeat = mock(CdcHeartbeatMonitor.class);
    private final ObjectMapper json = new ObjectMapper();

    /** 缓存可用时写 FRESH（写心跳失败时也是），否则写心跳状态；版本号是已加载的注册表版本。 */
    @ParameterizedTest
    @CsvSource({"true,FRESH,FRESH", "true,WRITE_FAILING,FRESH", "true,DISABLED,FRESH",
            "false,STALE,STALE", "false,STARTING,STARTING"})
    void writesStateAndLoadedRegistryVersion(boolean usable, State state, String expected) throws Exception {
        GroupRegistry registry = mock(GroupRegistry.class);
        when(registry.version()).thenReturn(7L);
        when(registry.loadAll()).thenReturn(Map.of());
        RegistryCacheGroups groups = new RegistryCacheGroups(registry);
        groups.refresh();
        when(redis.opsForValue()).thenReturn(values);
        when(heartbeat.cacheUsable()).thenReturn(usable);
        when(heartbeat.state()).thenReturn(state);

        new FreshnessPublisher(redis, json, heartbeat, groups, "cache-sync-service:18091", 3_000).publish();

        ArgumentCaptor<String> written = ArgumentCaptor.forClass(String.class);
        verify(values).set(eq(CacheKeys.FRESHNESS), written.capture(), eq(Duration.ofMillis(3_000)));
        FreshnessSignal signal = json.readValue(written.getValue(), FreshnessSignal.class);
        assertThat(signal).isEqualTo(new FreshnessSignal(expected, 7, "cache-sync-service:18091"));
        assertThat(signal.fresh()).isEqualTo(usable);
    }

    @ParameterizedTest
    @CsvSource({"true", "false"})
    void registryNotLoadedYetIsReportedAsMinusOne(boolean usable) throws Exception {
        RegistryCacheGroups groups = new RegistryCacheGroups(mock(GroupRegistry.class));
        when(redis.opsForValue()).thenReturn(values);
        when(heartbeat.cacheUsable()).thenReturn(usable);
        when(heartbeat.state()).thenReturn(State.STALE);

        new FreshnessPublisher(redis, json, heartbeat, groups, "i", 3_000).publish();

        ArgumentCaptor<String> written = ArgumentCaptor.forClass(String.class);
        verify(values).set(eq(CacheKeys.FRESHNESS), written.capture(), eq(Duration.ofMillis(3_000)));
        assertThat(json.readValue(written.getValue(), FreshnessSignal.class).registryVersion()).isEqualTo(-1);
    }
}
