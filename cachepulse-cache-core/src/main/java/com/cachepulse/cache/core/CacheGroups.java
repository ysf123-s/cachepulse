package com.cachepulse.cache.core;

import com.cachepulse.cache.model.AutoCacheConfig;

import java.util.Set;

/** 缓存分组配置的来源：接入方扫描本进程的 @AutoCache，同步服务读取 Redis 注册表。 */
public interface CacheGroups {
    /**
     * 按名字找分组。接入方只连一个业务库，名字就是表名；同步服务里还可以写成 库名.表名，
     * 同一个表名在多个库都注册了分组时必须这样写。
     */
    AutoCacheConfig getConfig(String tableName);

    /**
     * 按 库名.表名 找分组（设计文档第 6.6 节）：binlog 事件、新增主键广播都用它，别的库里同名的表不会匹配上。
     * 默认实现给只连一个库的接入方用：按表名找到后再核对库名；database 为 null 时只按表名找。
     */
    default AutoCacheConfig getConfig(String database, String table) {
        AutoCacheConfig config = getConfig(table);
        if (config == null || database == null || config.getDatabase() == null) {
            return config;
        }
        return database.equals(config.getDatabase()) ? config : null;
    }

    default boolean hasConfig(String tableName) {
        return getConfig(tableName) != null;
    }

    /**
     * 分组配置是否已经读到。同步服务从 Redis 注册表读，第一次读到之前为 false：这时收到的变更找不到分组，
     * 会被当成"没人管"确认掉，缓存留下旧值，所以同步服务要等它变成 true 才开始消费。接入方默认 true。
     */
    default boolean isLoaded() {
        return true;
    }

    /** 已注册分组的名字，可以直接传给 {@link #getConfig(String)}。 */
    Set<String> getAllTables();
}
