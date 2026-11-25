package com.cachepulse.cache.core;

import com.cachepulse.cache.model.AutoCacheConfig;

import java.util.Set;

/** 缓存分组配置的来源：接入方扫描本进程的 @AutoCache，同步服务读取 Redis 注册表。 */
public interface CacheGroups {
    AutoCacheConfig getConfig(String tableName);

    default boolean hasConfig(String tableName) {
        return getConfig(tableName) != null;
    }

    Set<String> getAllTables();
}
