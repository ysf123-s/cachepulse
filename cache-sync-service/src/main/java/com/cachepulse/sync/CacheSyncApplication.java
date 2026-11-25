package com.cachepulse.sync;

import com.cachepulse.cache.core.GroupRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

/** 缓存同步服务：消费 binlog 删缓存、心跳与丢失检测、发布新鲜度信号和新增主键。 */
@SpringBootApplication(scanBasePackages = {"com.cachepulse.sync", "com.cachepulse.common.parser", "com.cachepulse.common.kafka"})
@EnableScheduling
public class CacheSyncApplication {
    public static void main(String[] args) {
        SpringApplication.run(CacheSyncApplication.class, args);
    }

    @Bean
    public GroupRegistry groupRegistry(StringRedisTemplate redis, ObjectMapper json) {
        return new GroupRegistry(redis, json);
    }
}
