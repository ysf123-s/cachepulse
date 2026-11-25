package com.cachepulse.sync;

import com.cachepulse.cache.annotation.CacheStrategy;
import com.cachepulse.cache.core.CacheGroups;
import com.cachepulse.cache.model.AutoCacheConfig;
import com.cachepulse.common.kafka.CanalConnectorWrapper;
import com.cachepulse.common.model.BinlogEvent;
import com.cachepulse.common.model.BinlogVersion;
import com.cachepulse.common.parser.BinlogParser;
import com.cachepulse.common.parser.FlatMessageBinlogParser;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CanalCacheSyncServiceRecoveryTest {

    @Test
    void connectorUsesPartitionCount() {
        CanalConnectorWrapper wrapper = new CanalConnectorWrapper();
        ReflectionTestUtils.setField(wrapper, "partitionCount", 6);

        assertThat(wrapper.getConsumerThreadCount()).isEqualTo(6);
    }

    @Test
    void flatMessageModeDisablesVersionControl() throws Exception {
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        when(connector.isFlatMessageMode()).thenReturn(true);

        CanalCacheSyncService service = newService(connector);
        AutoCacheConfig config = config(true);

        assertThat(invokeShouldUseVersionControl(service, config)).isFalse();
    }

    @Test
    void entryModeKeepsVersionControlWhenConfigEnabled() throws Exception {
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        when(connector.isFlatMessageMode()).thenReturn(false);

        CanalCacheSyncService service = newService(connector);
        AutoCacheConfig config = config(true);

        assertThat(invokeShouldUseVersionControl(service, config)).isTrue();
    }

    @Test
    void entryModeStillDisablesVersionControlWhenConfigDisabled() throws Exception {
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        when(connector.isFlatMessageMode()).thenReturn(false);

        CanalCacheSyncService service = newService(connector);
        AutoCacheConfig config = config(false);

        assertThat(invokeShouldUseVersionControl(service, config)).isFalse();
    }

    @Test
    void processEventsHandlesPartitionBatchBeforeReturning() throws Exception {
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        when(connector.isFlatMessageMode()).thenReturn(false);
        CacheGroups processor = mock(CacheGroups.class);
        AutoCacheConfig config = config(true);
        config.setTable("users");
        config.setIdField("id");
        config.setKeyTemplate("user:#id");
        config.setStrategy(CacheStrategy.DELETE_MODE);
        config.setExpireSeconds(1800);
        VersionControllerWithLua versionController = mock(VersionControllerWithLua.class);
        when(versionController.deleteIfNewerVersion(anyString(), any(BinlogVersion.class)))
                .thenAnswer(invocation -> {
                    Thread.sleep(30);
                    return true;
                });
        when(processor.getConfig("users")).thenReturn(config);

        CanalCacheSyncService service = new CanalCacheSyncService(
                connector,
                new BinlogParser(),
                new FlatMessageBinlogParser(),
                processor,
                versionController,
                mock(BigKeyDetector.class),
                mock(RedisTemplate.class),
                new ObjectMapper(),
                new SimpleMeterRegistry()
        );

        List<BinlogEvent> events = List.of(
                event("1", 100L),
                event("2", 200L),
                event("3", 300L)
        );

        invokeProcessEvents(service, events);

        verify(versionController).deleteIfNewerVersion(
                eq("user:1"), eq(new BinlogVersion(0, 0, 100, 100)));
        verify(versionController).deleteIfNewerVersion(
                eq("user:2"), eq(new BinlogVersion(0, 0, 200, 200)));
        verify(versionController).deleteIfNewerVersion(
                eq("user:3"), eq(new BinlogVersion(0, 0, 300, 300)));
    }

    @Test
    void buildVersionPacksEpochFileNoAndOffsetIntoFiveSixteenThirtyTwoBits() throws Exception {
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        CanalCacheSyncService service = newService(connector);
        Method method = CanalCacheSyncService.class.getDeclaredMethod(
                "buildVersion",
                long.class,
                String.class,
                long.class
        );
        method.setAccessible(true);

        BinlogVersion version = (BinlogVersion) method.invoke(service, 1L, "mysql-bin.000003", 1000L);

        long expectedEncodedVersion = (1L << 48) | (3L << 32) | 1000L;
        assertThat(version).isEqualTo(new BinlogVersion(1L, 3L, 1000L, expectedEncodedVersion));
    }

    @Test
    void parseBinlogFileNoRejectsInvalidFormat() throws Exception {
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        CanalCacheSyncService service = newService(connector);
        Method method = CanalCacheSyncService.class.getDeclaredMethod("parseBinlogFileNo", String.class);
        method.setAccessible(true);

        assertThatThrownBy(() -> method.invoke(service, "mysql-bin.invalid"))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("Invalid binlog file name: mysql-bin.invalid");
    }

    private CanalCacheSyncService newService(CanalConnectorWrapper connector) {
        return newService(connector, mock(RedisTemplate.class));
    }

    private CanalCacheSyncService newService(CanalConnectorWrapper connector,
                                             RedisTemplate<String, String> redisTemplate) {
        return newService(connector, redisTemplate, mock(BigKeyDetector.class));
    }

    private CanalCacheSyncService newService(CanalConnectorWrapper connector,
                                             RedisTemplate<String, String> redisTemplate,
                                             BigKeyDetector bigKeyDetector) {
        return new CanalCacheSyncService(
                connector,
                new BinlogParser(),
                new FlatMessageBinlogParser(),
                mock(CacheGroups.class),
                mock(VersionControllerWithLua.class),
                bigKeyDetector,
                redisTemplate,
                new ObjectMapper(),
                new SimpleMeterRegistry()
        );
    }

    private boolean invokeShouldUseVersionControl(CanalCacheSyncService service,
                                                  AutoCacheConfig config) throws Exception {
        Method method = CanalCacheSyncService.class.getDeclaredMethod(
                "shouldUseVersionControl",
                AutoCacheConfig.class
        );
        method.setAccessible(true);
        return (boolean) method.invoke(service, config);
    }

    private void invokeProcessEvents(CanalCacheSyncService service,
                                     List<BinlogEvent> events) throws Exception {
        Method method = CanalCacheSyncService.class.getDeclaredMethod(
                "processEvents",
                List.class,
                List.class
        );
        method.setAccessible(true);
        method.invoke(service, events, new java.util.ArrayList<BinlogEvent>());
    }

    private BinlogEvent event(String id, long position) {
        return BinlogEvent.builder()
                .tableName("users")
                .eventType(EventType.UPDATE)
                .afterData(Map.of("id", id, "name", "Alice"))
                .binlogFile("mysql-bin.000000")
                .binlogPosition(position)
                .build();
    }

    private AutoCacheConfig config(boolean enableVersionControl) {
        AutoCacheConfig config = new AutoCacheConfig();
        config.setEnableVersionControl(enableVersionControl);
        return config;
    }
}
