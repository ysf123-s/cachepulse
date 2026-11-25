package com.cachepulse.sync;

import com.cachepulse.cache.core.CacheGroupDescriptor;
import com.cachepulse.cache.core.CacheGroups;
import com.cachepulse.cache.core.GroupRegistry;
import com.cachepulse.cache.model.AutoCacheConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * 同步服务的分组配置来自 Redis 注册表，每 2 秒检查一次版本号。先读版本再读内容，
 * 回报的版本不会比实际加载的内容新；接入方据此判断新分组的变更是否已经会被删缓存。
 */
@Slf4j
@Component
public class RegistryCacheGroups implements CacheGroups {
    private final GroupRegistry registry;
    private volatile Map<String, AutoCacheConfig> configs = Map.of();
    private volatile long loadedVersion = -1;

    public RegistryCacheGroups(GroupRegistry registry) {
        this.registry = registry;
    }

    @Scheduled(fixedDelayString = "${cachepulse.registry.refresh-ms:2000}", initialDelay = 0)
    public void refresh() {
        try {
            long version = registry.version();
            if (version == loadedVersion) {
                return;
            }
            Map<String, AutoCacheConfig> next = new LinkedHashMap<>();
            for (CacheGroupDescriptor group : registry.loadAll().values()) {
                next.put(group.table(), group.toConfig());
            }
            configs = Collections.unmodifiableMap(next);
            loadedVersion = version;
            log.info("Loaded cache group registry version {}: {}", version, next.keySet());
        } catch (RuntimeException e) {
            log.warn("Cannot load cache group registry: {}", e.toString());
        }
    }

    /** 已加载的注册表版本；还没加载成功时为 -1。 */
    public long loadedVersion() {
        return loadedVersion;
    }

    @Override
    public AutoCacheConfig getConfig(String tableName) {
        return configs.get(tableName);
    }

    @Override
    public Set<String> getAllTables() {
        return configs.keySet();
    }
}
