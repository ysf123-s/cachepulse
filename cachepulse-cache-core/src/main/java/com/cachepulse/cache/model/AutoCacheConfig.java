package com.cachepulse.cache.model;

import com.cachepulse.cache.annotation.CacheStrategy;

import lombok.Data;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

/**
 * 缓存配置信息
 */
@Data
public class AutoCacheConfig {

    /** 业务库名。同步服务按 库名.表名 匹配 binlog 事件。 */
    private String database;
    private String table;
    private Class<?> entityClass;
    private String idField;
    private Class<?> idFieldType;
    private String keyTemplate;
    private CacheStrategy strategy;
    private int expireSeconds;
    private int maxSizeBytes;
    private boolean enableVersionControl;
    private List<IndexKey> indexKeys = List.of();
    /** 写入缓存时在过期时间上随机增加 0–ttlJitterPercent%，由全局配置 cache.ttl-jitter-percent 设置。 */
    private int ttlJitterPercent;
    /** 是否为主键维护布隆过滤器，判定一定不存在的主键查库时走窄通道。 */
    private boolean bloomFilter;

    /** 在 seconds 上随机增加 0–ttlJitterPercent%，让同一批写入的缓存错开过期，避免同时失效后集中查库。 */
    public int jitteredSeconds(int seconds) {
        int spread = (int) ((long) seconds * ttlJitterPercent / 100);
        return spread <= 0 ? seconds : seconds + ThreadLocalRandom.current().nextInt(spread + 1);
    }

    /**
     * 根据 keyTemplate 和 id 值生成缓存 Key，例如 "user:#id" + 42 → "user:42"
     */
    public String buildCacheKeySimple(Object idValue) {
        return keyTemplate.replace("#id", String.valueOf(idValue));
    }

    /** 配置注册时已校验：固定命名空间后只能是末尾的 #id。 */
    public String keyPrefix() {
        return keyTemplate.substring(0, keyTemplate.length() - "#id".length());
    }

    public IndexKey indexKey(String column) {
        return indexKeys.stream().filter(index -> index.column().equals(column)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No index on " + table + "." + column));
    }

    /** 本分组占用的全部命名空间：主键缓存加各二级索引缓存。 */
    public List<String> allPrefixes() {
        return Stream.concat(Stream.of(keyPrefix()), indexKeys.stream().map(IndexKey::prefix)).toList();
    }

    /** 二级索引缓存模板，例如 "user-email:#email" + "a@x.com" → "user-email:a@x.com"。列值按原样拼接，精确匹配。 */
    public record IndexKey(String template, String column) {
        public static IndexKey parse(String template) {
            return new IndexKey(template, template.substring(template.lastIndexOf(":#") + 2));
        }

        public String prefix() {
            return template.substring(0, template.length() - column.length() - 1);
        }

        public String buildKey(Object value) {
            return prefix() + value;
        }
    }
}
