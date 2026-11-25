package com.cachepulse.sync;

import com.cachepulse.cache.annotation.CacheStrategy;
import com.cachepulse.cache.core.CacheGroupDescriptor;
import com.cachepulse.cache.core.GroupRegistry;
import com.cachepulse.common.kafka.CanalConnectorWrapper;
import com.cachepulse.common.model.BinlogEvent;
import com.cachepulse.common.parser.BinlogParser;
import com.cachepulse.common.parser.FlatMessageBinlogParser;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/** 分组来自 Redis 注册表：多个分组共用同一个同步流程，各用各的 key 和过期时间。 */
class MultiTableCacheTest {
    static CacheGroupDescriptor group(String table, String keyTemplate, int expireSeconds, String... indexKeys) {
        return new CacheGroupDescriptor("demo-user-service", "demo_db", table, "id", keyTemplate,
                List.of(indexKeys), CacheStrategy.DELETE_MODE, expireSeconds, 10240, false, 10, true);
    }

    @Test
    void registeredTablesShareInvalidationWithSeparateKeysAndTtls() {
        GroupRegistry registry = mock(GroupRegistry.class);
        when(registry.version()).thenReturn(1L);
        when(registry.loadAll()).thenReturn(Map.of(
                "demo_db.users", group("users", "user:#id", 1800, "user-email:#email"),
                "demo_db.departments", group("departments", "department:#id", 600)));
        RegistryCacheGroups groups = new RegistryCacheGroups(registry);
        groups.refresh();

        assertThat(groups.getAllTables()).containsExactlyInAnyOrder("users", "departments");
        assertThat(groups.getConfig("departments").getStrategy()).isEqualTo(CacheStrategy.DELETE_MODE);
        assertThat(groups.getConfig("departments").getExpireSeconds()).isEqualTo(600);
        assertThat(groups.getConfig("users").getExpireSeconds()).isEqualTo(1800);

        RedisTemplate<String, String> redis = mock(RedisTemplate.class);
        SimpleMeterRegistry metrics = new SimpleMeterRegistry();
        CanalCacheSyncService sync = new CanalCacheSyncService(mock(CanalConnectorWrapper.class),
                new BinlogParser(), new FlatMessageBinlogParser(), groups,
                new VersionControllerWithLua(redis), mock(BigKeyDetector.class), redis, new ObjectMapper(), metrics);
        sync.handleEvent(BinlogEvent.builder().databaseName("demo_db").tableName("users")
                .eventType(CanalEntry.EventType.UPDATE).afterData(Map.of("id", "1", "name", "new")).build());
        sync.handleEvent(BinlogEvent.builder().databaseName("demo_db").tableName("departments")
                .eventType(CanalEntry.EventType.UPDATE).afterData(Map.of("id", "1", "name", "new")).build());

        verify(redis).delete("user:1");
        verify(redis).delete("department:1");
        assertThat(metrics.get("canal.events.processed").counter().count()).isEqualTo(2);
    }
}
