package com.cachepulse.cache.starter;

import com.cachepulse.cache.core.CacheGroups;
import com.cachepulse.cache.model.AutoCacheConfig;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.DefaultMessage;

import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.*;

/** 新增主键广播按 库名.表名 投递：只有本服务所连库里注册过的分组才加入布隆过滤器（设计文档第 6.3、6.6 节）。 */
class BloomBroadcastListenerTest {
    private final PrimaryKeyFilters filters = mock(PrimaryKeyFilters.class);
    private final BloomBroadcastListener listener;

    BloomBroadcastListenerTest() {
        AutoCacheConfig users = new AutoCacheConfig();
        users.setDatabase("demo_db");
        users.setTable("users");
        CacheGroups groups = mock(CacheGroups.class, CALLS_REAL_METHODS);
        when(groups.getConfig("users")).thenReturn(users);
        listener = new BloomBroadcastListener(groups, filters);
    }

    @Test
    void addsKeysOfOwnDatabaseAndTable() {
        listener.onMessage(message("cachepulse:bloom:demo_db.users", "42"), null);

        verify(filters).add("users", 42L);
    }

    @Test
    void ignoresSameTableNameInAnotherDatabaseUnknownTablesAndOldChannelFormat() {
        listener.onMessage(message("cachepulse:bloom:other_db.users", "42"), null);
        listener.onMessage(message("cachepulse:bloom:demo_db.orders", "42"), null);
        listener.onMessage(message("cachepulse:bloom:users", "42"), null);
        listener.onMessage(message("cachepulse:bloom:demo_db.users", "not-a-number"), null);

        verifyNoInteractions(filters);
    }

    private static DefaultMessage message(String channel, String body) {
        return new DefaultMessage(channel.getBytes(StandardCharsets.UTF_8), body.getBytes(StandardCharsets.UTF_8));
    }
}
