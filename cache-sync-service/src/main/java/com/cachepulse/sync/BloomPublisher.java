package com.cachepulse.sync;

import com.cachepulse.cache.core.CacheGroups;
import com.cachepulse.cache.core.CacheKeys;
import com.cachepulse.cache.core.PrimaryKeySink;
import com.cachepulse.cache.model.AutoCacheConfig;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/** CDC 发现新增主键后广播给接入方的布隆过滤器。广播丢失不影响正确性，只是刚插入的行多走一次窄通道。 */
@Component
public class BloomPublisher implements PrimaryKeySink {
    private final StringRedisTemplate redis;
    private final CacheGroups groups;

    public BloomPublisher(StringRedisTemplate redis, CacheGroups groups) {
        this.redis = redis;
        this.groups = groups;
    }

    @Override
    public void add(String database, String table, Object id) {
        AutoCacheConfig config = groups.getConfig(database, table);
        if (config != null && config.isBloomFilter() && id != null) {
            String db = config.getDatabase() != null ? config.getDatabase() : database;
            redis.convertAndSend(CacheKeys.bloomChannel(db, config.getTable() != null ? config.getTable() : table),
                    String.valueOf(id));
        }
    }
}
