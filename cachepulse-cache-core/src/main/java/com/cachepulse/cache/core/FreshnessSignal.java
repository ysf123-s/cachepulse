package com.cachepulse.cache.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;

/**
 * 同步服务写入、接入方读取的新鲜度信号。用 Redis 过期时间判断信号是否失效，不比较两台机器的时钟：
 * 信号不存在（同步服务停了）和状态不是 FRESH 一样，接入方都绕过缓存直接查库。
 */
public record FreshnessSignal(String state, long registryVersion, String instance) {

    public boolean fresh() {
        return "FRESH".equals(state);
    }

    public void write(StringRedisTemplate redis, ObjectMapper json, Duration ttl) {
        try {
            redis.opsForValue().set(CacheKeys.FRESHNESS, json.writeValueAsString(this), ttl);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot encode freshness signal", e);
        }
    }

    public static FreshnessSignal read(StringRedisTemplate redis, ObjectMapper json) {
        String value = redis.opsForValue().get(CacheKeys.FRESHNESS);
        if (value == null) {
            return null;
        }
        try {
            return json.readValue(value, FreshnessSignal.class);
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}
