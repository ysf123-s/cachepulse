package com.cachepulse.sync;

import com.cachepulse.cache.core.CacheGroupDescriptor;
import com.cachepulse.cache.core.CacheGroups;
import com.cachepulse.cache.core.GroupRegistry;
import com.cachepulse.cache.model.AutoCacheConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 同步服务的分组配置来自 Redis 注册表，每 2 秒检查一次版本号。先读版本再读内容，
 * 回报的版本不会比实际加载的内容新；接入方据此判断新分组的变更是否已经会被删缓存。
 *
 * <p>分组按 库名.表名 存（设计文档第 6.6 节）：不同库里的同名表各是一个分组，互不覆盖。
 * 管理接口可以只写表名，同一个表名在多个库都有分组时要写成 库名.表名。</p>
 */
@Slf4j
@Component
public class RegistryCacheGroups implements CacheGroups {
    private final GroupRegistry registry;
    private volatile Map<String, AutoCacheConfig> configs = Map.of();
    private volatile Map<String, List<AutoCacheConfig>> byTable = Map.of();
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
            Map<String, List<AutoCacheConfig>> tables = new LinkedHashMap<>();
            for (CacheGroupDescriptor group : registry.loadAll().values()) {
                AutoCacheConfig config = group.toConfig();
                next.put(group.key(), config);
                tables.computeIfAbsent(group.table(), table -> new ArrayList<>()).add(config);
            }
            tables.replaceAll((table, list) -> List.copyOf(list));
            configs = Collections.unmodifiableMap(next);
            byTable = Collections.unmodifiableMap(tables);
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
    public AutoCacheConfig getConfig(String database, String table) {
        if (database == null) {
            List<AutoCacheConfig> matches = byTable.getOrDefault(table, List.of());
            return matches.size() == 1 ? matches.get(0) : null;
        }
        return configs.get(database + "." + table);
    }

    /** 表名或 库名.表名；表名在多个库都有分组时抛出 IllegalArgumentException（管理接口返回 400）。 */
    @Override
    public AutoCacheConfig getConfig(String name) {
        if (name == null) {
            return null;
        }
        AutoCacheConfig qualified = configs.get(name);
        if (qualified != null) {
            return qualified;
        }
        List<AutoCacheConfig> matches = byTable.getOrDefault(name, List.of());
        if (matches.size() > 1) {
            throw new IllegalArgumentException("Table " + name + " has cache groups in several databases; use database.table");
        }
        return matches.isEmpty() ? null : matches.get(0);
    }

    /** 表名唯一时用表名（管理页面照旧），同一个表名在多个库都有分组时用 库名.表名。 */
    @Override
    public Set<String> getAllTables() {
        Set<String> names = new LinkedHashSet<>();
        byTable.forEach((table, list) -> {
            if (list.size() == 1) {
                names.add(table);
            } else {
                list.forEach(config -> names.add(config.getDatabase() + "." + table));
            }
        });
        return Collections.unmodifiableSet(names);
    }
}
