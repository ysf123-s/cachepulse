package com.cachepulse.sync;

import com.cachepulse.cache.core.CacheGroups;
import com.cachepulse.cache.core.PrimaryKeySink;
import com.cachepulse.cache.model.AutoCacheConfig;
import com.cachepulse.common.kafka.CanalConnectorWrapper;
import com.cachepulse.common.model.BinlogEvent;
import com.cachepulse.common.parser.BinlogParser;
import com.cachepulse.common.parser.FlatMessageBinlogParser;
import com.cachepulse.sync.diagnostics.TraceCollector;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Map;

import static org.mockito.Mockito.*;

/** 设计文档第 6.6 节：分组按 库名.表名 匹配，别的库里同名的表不能删缓存。 */
class DatabaseScopedInvalidationTest {
    private final RedisTemplate<String, String> redis = mock(RedisTemplate.class);
    private final PrimaryKeySink primaryKeys = mock(PrimaryKeySink.class);
    private final CanalCacheSyncService sync;

    DatabaseScopedInvalidationTest() {
        AutoCacheConfig users = MultiTableCacheTest.group("users", "user:#id", 1800, "user-email:#email").toConfig();
        CacheGroups groups = mock(CacheGroups.class, CALLS_REAL_METHODS);
        when(groups.getConfig("users")).thenReturn(users);
        SimpleMeterRegistry metrics = new SimpleMeterRegistry();
        ObjectMapper json = new ObjectMapper();
        sync = new CanalCacheSyncService(mock(CanalConnectorWrapper.class), new BinlogParser(),
                new FlatMessageBinlogParser(), groups, mock(VersionControllerWithLua.class), mock(BigKeyDetector.class),
                redis, json, metrics, new TraceCollector(), mock(DeadLetterStore.class),
                mock(CacheNamespacePurger.class), CdcHeartbeatMonitor.disabled(), primaryKeys);
    }

    @ParameterizedTest
    @EnumSource(value = EventType.class, names = {"INSERT", "UPDATE", "DELETE"})
    void sameTableNameInAnotherDatabaseDoesNotInvalidate(EventType type) {
        sync.handleEvent(event("other_db", type));

        verifyNoInteractions(redis, primaryKeys);
    }

    @ParameterizedTest
    @EnumSource(value = EventType.class, names = {"INSERT", "UPDATE", "DELETE"})
    void registeredDatabaseInvalidates(EventType type) {
        sync.handleEvent(event("demo_db", type));

        verify(redis).delete("user:1");
    }

    private static BinlogEvent event(String database, EventType type) {
        Map<String, Object> row = Map.of("id", "1", "email", "a@x.com");
        return BinlogEvent.builder().databaseName(database).tableName("users").eventType(type)
                .beforeData(type == EventType.INSERT ? null : row).afterData(type == EventType.DELETE ? null : row)
                .build();
    }
}
