package com.cachepulse.cache.core;

/** 组件保留的 Redis key 和频道。业务缓存分组的命名空间不能和它们重叠。 */
public final class CacheKeys {
    public static final String RESERVED_PREFIX = "cachepulse:";
    public static final String REGISTRY_GROUPS = RESERVED_PREFIX + "registry:groups";
    public static final String REGISTRY_VERSION = RESERVED_PREFIX + "registry:version";
    public static final String FRESHNESS = RESERVED_PREFIX + "freshness";
    public static final String BLOOM_CHANNEL_PREFIX = RESERVED_PREFIX + "bloom:";
    public static final String DEAD_LETTERS = RESERVED_PREFIX + "dead-letters";
    public static final String HEARTBEAT_CHAIN = RESERVED_PREFIX + "heartbeat-chain";
    /** 不存在的行按空值缓存的秒数，管理页面展示用。 */
    public static final int NULL_CACHE_SECONDS = 30;

    private CacheKeys() { }

    public static String bloomChannel(String table) {
        return BLOOM_CHANNEL_PREFIX + table;
    }

    /** 主键缓存与二级索引缓存共用整表清空和浏览，任意两个命名空间之间不能互为前缀。 */
    public static void requireDisjoint(String prefix, String other) {
        if (prefix.startsWith(other) || other.startsWith(prefix)) {
            throw new IllegalStateException("Overlapping cache prefixes: " + prefix + " and " + other);
        }
    }

    public static void requireNotReserved(String prefix) {
        if (RESERVED_PREFIX.startsWith(prefix) || prefix.startsWith(RESERVED_PREFIX)) {
            throw new IllegalStateException("Cache prefix overlaps reserved namespace " + RESERVED_PREFIX + ": " + prefix);
        }
    }
}
