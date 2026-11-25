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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** 跨进程注册的分组只有删除策略（设计文档第 6.9 节）。 */
class CachePrimaryKeyChangeTest {
    @Test
    void primaryKeyChangeDeletesOldAndNewKey() {
        var fixture = new Fixture(false);
        fixture.sync.handleEvent(update(1L, "2"));
        verify(fixture.redis).delete("user:1");
        verify(fixture.redis).delete("user:2");
    }

    @Test
    void primaryKeyChangeUsesSameBinlogVersionForOldAndNewKey() {
        var fixture = new Fixture(true);
        fixture.sync.handleEvent(update(1L, "2"));
        var version = ArgumentCaptor.forClass(BinlogVersion.class);
        verify(fixture.versions).deleteIfNewerVersion(eq("user:1"), version.capture());
        verify(fixture.versions).deleteIfNewerVersion("user:2", version.getValue());
        verifyNoInteractions(fixture.redis);
    }

    @ParameterizedTest
    @CsvSource({"1,1", "1,2"})
    void ordinaryUpdateDeletesEachCacheKeyOnlyOnce(long before, long after) {
        var fixture = new Fixture(false);
        fixture.sync.handleEvent(update(before, Long.toString(after)));
        verify(fixture.redis).delete("user:" + after);
        if (before != after) verify(fixture.redis).delete("user:" + before);
        verifyNoMoreInteractions(fixture.redis);
    }

    @Test
    void minimalRowImageUpdateUsesBeforeImageKeyAndOnlyDeletes() {
        var fixture = new Fixture(false);
        fixture.sync.handleEvent(BinlogEvent.builder().tableName("users").eventType(EventType.UPDATE)
                .beforeData(Map.of("id", "1", "name", "before")).afterData(Map.of("name", "after")).build());
        verify(fixture.redis).delete("user:1");
        verifyNoMoreInteractions(fixture.redis);
        verifyNoInteractions(fixture.versions);
    }

    private static BinlogEvent update(Object before, Object after) {
        return BinlogEvent.builder().tableName("users").eventType(EventType.UPDATE)
                .beforeData(Map.of("id", before, "name", "before"))
                .afterData(Map.of("id", after, "name", "after"))
                .binlogFile("binlog.000002").binlogPosition(100L).build();
    }

    private static class Fixture {
        final RedisTemplate<String, String> redis = mock(RedisTemplate.class);
        final VersionControllerWithLua versions = mock(VersionControllerWithLua.class);
        final CanalCacheSyncService sync;
        Fixture(boolean versionControl) {
            var config = new AutoCacheConfig();
            config.setTable("users"); config.setIdField("id"); config.setKeyTemplate("user:#id");
            config.setStrategy(CacheStrategy.DELETE_MODE); config.setExpireSeconds(1800);
            config.setEnableVersionControl(versionControl);
            var groups = mock(CacheGroups.class);
            when(groups.getConfig("users")).thenReturn(config);
            sync = new CanalCacheSyncService(mock(CanalConnectorWrapper.class), new BinlogParser(),
                    new FlatMessageBinlogParser(), groups, versions, mock(BigKeyDetector.class), redis,
                    new ObjectMapper(), new SimpleMeterRegistry());
        }
    }
}
