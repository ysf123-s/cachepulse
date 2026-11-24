package com.example.canalcache.service;

/** 查询侧判断缓存此刻能否信任；返回 false 时查询绕过 Redis 直接读库。 */
@FunctionalInterface
public interface CacheFreshness {
    CacheFreshness ALWAYS = () -> true;

    boolean cacheUsable();
}
