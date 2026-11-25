package com.cachepulse.sync;

import com.cachepulse.cache.annotation.CacheStrategy;
import com.cachepulse.cache.core.CacheGroups;
import com.cachepulse.cache.model.AutoCacheConfig;
import com.cachepulse.common.kafka.CanalConnectorWrapper;
import com.cachepulse.common.model.BinlogEvent;
import com.cachepulse.common.parser.BinlogParser;
import com.cachepulse.common.parser.FlatMessageBinlogParser;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CacheIndexInvalidationTest {
    private final RedisTemplate<String, String> redis = mock(RedisTemplate.class);
    private final VersionControllerWithLua versions = mock(VersionControllerWithLua.class);

    @Test
    void emailChangeDeletesOldAndNewIndexKeys() {
        sync().handleEvent(event(EventType.UPDATE,
                Map.of("id", "1", "email", "a@x.com"), Map.of("id", "1", "email", "b@x.com")));
        verify(redis).delete("user:1");
        verify(redis).delete(List.of("user-email:a@x.com", "user-email:b@x.com"));
    }


    @Test
    void updateThatKeepsEmailAndIdLeavesIndexAlone() {
        sync().handleEvent(event(EventType.UPDATE,
                Map.of("id", "1", "email", "a@x.com", "name", "old"),
                Map.of("id", "1", "email", "a@x.com", "name", "new")));
        verify(redis).delete("user:1");
        verifyNoMoreInteractions(redis);
    }

    @Test
    void primaryKeyChangeWithSameEmailStillDeletesIndexBecauseIdListChanged() {
        sync().handleEvent(event(EventType.UPDATE,
                Map.of("id", "1", "email", "a@x.com"), Map.of("id", "2", "email", "a@x.com")));
        verify(redis).delete(List.of("user-email:a@x.com"));
    }

    @Test
    void insertClearsEmptyListCachedForTheNewEmail() {
        sync().handleEvent(event(EventType.INSERT, null, Map.of("id", "2", "email", "c@x.com")));
        verify(redis).delete("user:2");
        verify(redis).delete(List.of("user-email:c@x.com"));
    }

    @Test
    void deleteUsesBeforeImageEmail() {
        sync().handleEvent(event(EventType.DELETE, Map.of("id", "3", "email", "d@x.com"), null));
        verify(redis).delete("user:3");
        verify(redis).delete(List.of("user-email:d@x.com"));
    }

    @Test
    void minimalRowImageWithoutEmailInAfterImageDeletesBeforeImageKey() {
        sync().handleEvent(event(EventType.UPDATE,
                Map.of("id", "1", "email", "a@x.com"), Map.of("name", "new")));
        verify(redis).delete("user:1");
        verify(redis).delete(List.of("user-email:a@x.com"));
    }

    @Test
    void nullEmailHasNoIndexKey() {
        Map<String, Object> after = new HashMap<>(Map.of("id", "4"));
        after.put("email", null);
        sync().handleEvent(event(EventType.INSERT, null, after));
        verify(redis).delete("user:4");
        verifyNoMoreInteractions(redis);
    }

    @Test
    void indexDeletionFailurePropagatesSoTheBatchIsRetried() {
        when(redis.delete(anyCollection())).thenThrow(new RedisConnectionFailureException("down"));
        assertThatThrownBy(() -> sync().handleEvent(
                        event(EventType.INSERT, null, Map.of("id", "2", "email", "c@x.com"))))
                .isInstanceOf(RedisConnectionFailureException.class);
    }

    private CanalCacheSyncService sync() {
        AutoCacheConfig config = new AutoCacheConfig();
        config.setTable("users");
        config.setIdField("id");
        config.setKeyTemplate("user:#id");
        config.setIndexKeys(List.of(AutoCacheConfig.IndexKey.parse("user-email:#email")));
        config.setStrategy(CacheStrategy.DELETE_MODE);
        config.setExpireSeconds(1800);
        CacheGroups processor = mock(CacheGroups.class, CALLS_REAL_METHODS);
        when(processor.getConfig("users")).thenReturn(config);
        return new CanalCacheSyncService(mock(CanalConnectorWrapper.class), new BinlogParser(),
                new FlatMessageBinlogParser(), processor, versions, mock(BigKeyDetector.class), redis,
                new ObjectMapper(), new SimpleMeterRegistry());
    }

    private static BinlogEvent event(EventType type, Map<String, Object> before, Map<String, Object> after) {
        return BinlogEvent.builder().tableName("users").eventType(type).beforeData(before).afterData(after)
                .binlogFile("binlog.000002").binlogPosition(100L).build();
    }
}
