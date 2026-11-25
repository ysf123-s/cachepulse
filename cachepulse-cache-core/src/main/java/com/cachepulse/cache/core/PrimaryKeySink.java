package com.cachepulse.cache.core;

/** 接收 CDC 发现的新增主键：同步服务把它广播出去，接入方加入本地布隆过滤器。 */
@FunctionalInterface
public interface PrimaryKeySink {
    PrimaryKeySink NONE = (table, id) -> { };

    void add(String table, Object id);

    static PrimaryKeySink none() {
        return NONE;
    }
}
