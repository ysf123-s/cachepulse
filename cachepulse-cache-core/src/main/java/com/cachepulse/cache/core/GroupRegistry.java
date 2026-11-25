package com.cachepulse.cache.core;

import com.cachepulse.cache.annotation.CacheStrategy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Redis 中的缓存分组注册表：字段为 库名.表名，值为分组定义；版本号在新增分组时递增。
 * 接入方启动时注册，同步服务定期读取，并在新鲜度信号里回报已加载的版本。
 */
public class GroupRegistry {
    private final StringRedisTemplate redis;
    private final ObjectMapper json;

    public GroupRegistry(StringRedisTemplate redis, ObjectMapper json) {
        this.redis = redis;
        this.json = json;
    }

    /** 注册本服务的分组，返回同步服务需要达到的注册表版本。定义冲突时抛出，让接入方启动失败。 */
    public long register(List<CacheGroupDescriptor> groups) {
        Map<String, CacheGroupDescriptor> existing = loadAll();
        boolean changed = false;
        for (CacheGroupDescriptor group : groups) {
            if (group.strategy() != CacheStrategy.DELETE_MODE) {
                throw new IllegalStateException("Cache group " + group.key()
                        + " uses " + group.strategy() + "; services registered across processes support DELETE_MODE only");
            }
            CacheGroupDescriptor current = existing.get(group.key());
            if (current != null) {
                requireSame(current, group);
                continue;
            }
            for (CacheGroupDescriptor other : existing.values()) {
                for (String prefix : group.toConfig().allPrefixes()) {
                    for (String otherPrefix : other.toConfig().allPrefixes()) {
                        CacheKeys.requireDisjoint(prefix, otherPrefix);
                    }
                }
            }
            if (!Boolean.TRUE.equals(redis.opsForHash().putIfAbsent(CacheKeys.REGISTRY_GROUPS, group.key(), encode(group)))) {
                requireSame(loadAll().get(group.key()), group);
                continue;
            }
            existing.put(group.key(), group);
            changed = true;
        }
        if (changed) {
            Long version = redis.opsForValue().increment(CacheKeys.REGISTRY_VERSION);
            return version == null ? version() : version;
        }
        return version();
    }

    public Map<String, CacheGroupDescriptor> loadAll() {
        Map<String, CacheGroupDescriptor> result = new LinkedHashMap<>();
        redis.<String, String>opsForHash().entries(CacheKeys.REGISTRY_GROUPS)
                .forEach((key, value) -> result.put(key, decode(value)));
        return result;
    }

    public long version() {
        String value = redis.opsForValue().get(CacheKeys.REGISTRY_VERSION);
        return value == null ? 0 : Long.parseLong(value);
    }

    private static void requireSame(CacheGroupDescriptor current, CacheGroupDescriptor group) {
        if (current != null && !current.sameDefinition(group)) {
            throw new IllegalStateException("Cache group " + group.key() + " is already registered by "
                    + current.owner() + " with a different definition");
        }
    }

    private String encode(CacheGroupDescriptor group) {
        try {
            return json.writeValueAsString(group);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot encode cache group " + group.key(), e);
        }
    }

    private CacheGroupDescriptor decode(String value) {
        try {
            return json.readValue(value, CacheGroupDescriptor.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Corrupted cache group registry entry: " + value, e);
        }
    }
}
