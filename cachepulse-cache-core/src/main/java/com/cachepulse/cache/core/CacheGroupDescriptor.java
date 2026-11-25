package com.cachepulse.cache.core;

import com.cachepulse.cache.annotation.CacheStrategy;
import com.cachepulse.cache.model.AutoCacheConfig;

import java.util.List;
import java.util.Objects;

/** 写入 Redis 注册表的分组定义，不含实体类，同步服务据此删缓存。 */
public record CacheGroupDescriptor(String owner, String database, String table, String idField, String keyTemplate,
                                   List<String> indexKeys, CacheStrategy strategy, int expireSeconds, int maxSizeBytes,
                                   boolean enableVersionControl, int ttlJitterPercent, boolean bloomFilter) {

    public static CacheGroupDescriptor of(String owner, AutoCacheConfig config) {
        return new CacheGroupDescriptor(owner, config.getDatabase(), config.getTable(), config.getIdField(),
                config.getKeyTemplate(), config.getIndexKeys().stream().map(AutoCacheConfig.IndexKey::template).toList(),
                config.getStrategy(), config.getExpireSeconds(), config.getMaxSizeBytes(),
                config.isEnableVersionControl(), config.getTtlJitterPercent(), config.isBloomFilter());
    }

    public String key() {
        return database + "." + table;
    }

    /** 同一分组可以被同一服务的多个实例重复注册，只要定义完全一致。 */
    public boolean sameDefinition(CacheGroupDescriptor other) {
        return Objects.equals(owner, other.owner) && Objects.equals(database, other.database)
                && Objects.equals(table, other.table) && Objects.equals(idField, other.idField)
                && Objects.equals(keyTemplate, other.keyTemplate) && Objects.equals(indexKeys, other.indexKeys)
                && strategy == other.strategy && expireSeconds == other.expireSeconds
                && maxSizeBytes == other.maxSizeBytes && enableVersionControl == other.enableVersionControl
                && ttlJitterPercent == other.ttlJitterPercent && bloomFilter == other.bloomFilter;
    }

    public AutoCacheConfig toConfig() {
        AutoCacheConfig config = new AutoCacheConfig();
        config.setDatabase(database);
        config.setTable(table);
        config.setIdField(idField);
        config.setIdFieldType(Long.class);
        config.setKeyTemplate(keyTemplate);
        config.setIndexKeys(indexKeys.stream().map(AutoCacheConfig.IndexKey::parse).toList());
        config.setStrategy(strategy);
        config.setExpireSeconds(expireSeconds);
        config.setMaxSizeBytes(maxSizeBytes);
        config.setEnableVersionControl(enableVersionControl);
        config.setTtlJitterPercent(ttlJitterPercent);
        config.setBloomFilter(bloomFilter);
        return config;
    }
}
