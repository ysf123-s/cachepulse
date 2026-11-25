package com.cachepulse.cache.starter;

import com.cachepulse.cache.model.AutoCacheConfig;

import org.springframework.stereotype.Component;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Spring 收集加载器 Bean；启动时确认每个缓存分组恰好对应一个相同类型的加载器。 */
@Component
public class CacheLoaderRegistry {
    private final Map<String, CacheDataLoader<?>> loaders = new LinkedHashMap<>();

    public CacheLoaderRegistry(List<CacheDataLoader<?>> candidates, CacheAnnotationProcessor processor) {
        for (CacheDataLoader<?> loader : candidates) {
            AutoCacheConfig config = processor.getConfig(loader.table());
            if (config == null) throw new IllegalStateException("Loader table is not registered: " + loader.table());
            if (config.getEntityClass() != loader.entityType()) {
                throw new IllegalStateException("Loader entity type mismatch for " + loader.table());
            }
            if (loaders.putIfAbsent(loader.table(), loader) != null) {
                throw new IllegalStateException("Duplicate cache loader for " + loader.table());
            }
        }
        for (String table : processor.getAllTables()) {
            if (!loaders.containsKey(table)) throw new IllegalStateException("Missing cache loader for " + table);
        }
    }

    /** 不关心实体类型的用途（如扫描主键）；未注册时返回 null。 */
    public CacheDataLoader<?> get(String table) {
        return loaders.get(table);
    }

    @SuppressWarnings("unchecked")
    public <T> CacheDataLoader<T> get(String table, Class<T> type) {
        CacheDataLoader<?> loader = loaders.get(table);
        if (loader == null) throw new IllegalArgumentException("Unknown cache group: " + table);
        if (loader.entityType() != type) throw new IllegalArgumentException("Wrong entity type for " + table);
        return (CacheDataLoader<T>) loader;
    }
}
