package com.cachepulse.sync;

import com.cachepulse.cache.core.FreshnessSignal;
import com.cachepulse.sync.CdcHeartbeatMonitor.State;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 每 200ms 把心跳状态和已加载的注册表版本写到 Redis，过期时间 3 秒。同步服务停了，信号 3 秒内消失，
 * 接入方随即绕过缓存。第一阶段只部署一个实例（设计文档第 6.11 节），多实例会互相覆盖。
 */
@Slf4j
@Component
public class FreshnessPublisher {
    private final StringRedisTemplate redis;
    private final ObjectMapper json;
    private final CdcHeartbeatMonitor heartbeat;
    private final RegistryCacheGroups groups;
    private final String instance;
    private final Duration ttl;

    public FreshnessPublisher(StringRedisTemplate redis, ObjectMapper json, CdcHeartbeatMonitor heartbeat,
                              RegistryCacheGroups groups,
                              @Value("${spring.application.name}:${server.port:0}") String instance,
                              @Value("${cachepulse.freshness.ttl-ms:3000}") long ttlMs) {
        this.redis = redis;
        this.json = json;
        this.heartbeat = heartbeat;
        this.groups = groups;
        this.instance = instance;
        this.ttl = Duration.ofMillis(ttlMs);
    }

    @Scheduled(fixedDelayString = "${cachepulse.freshness.publish-ms:200}")
    public void publish() {
        State state = heartbeat.state();
        String signalState = state == State.DISABLED ? State.FRESH.name() : state.name();
        try {
            new FreshnessSignal(signalState, groups.loadedVersion(), instance).write(redis, json, ttl);
        } catch (RuntimeException e) {
            log.debug("Cannot publish freshness signal: {}", e.toString());
        }
    }
}
