package com.cachepulse.cache.starter;

import com.cachepulse.cache.core.GroupRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 启动时把本服务的缓存分组注册到 Redis，之后每 30 秒复查一次：Redis 被清空后重新注册。
 * 定义冲突在启动时抛出，让服务启动失败；运行中复查失败只记日志，缓存保持不可用直到注册成功。
 */
@Slf4j
public class CacheGroupRegistrar implements AutoCloseable {
    private final CacheAnnotationProcessor groups;
    private final GroupRegistry registry;
    private final RemoteCacheFreshness freshness;
    private final String owner;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "cachepulse-registrar");
        thread.setDaemon(true);
        return thread;
    });

    public CacheGroupRegistrar(CacheAnnotationProcessor groups, GroupRegistry registry,
                               RemoteCacheFreshness freshness, String owner) {
        this.groups = groups;
        this.registry = registry;
        this.freshness = freshness;
        this.owner = owner;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        register();
        scheduler.scheduleWithFixedDelay(this::recheck, 30, 30, TimeUnit.SECONDS);
    }

    void register() {
        long version = registry.register(groups.descriptors(owner));
        freshness.requireRegistryVersion(version);
        log.info("Registered {} cache groups as {}; waiting for cache-sync to load registry version {}",
                groups.getAllTables().size(), owner, version);
    }

    private void recheck() {
        try {
            if (!registry.loadAll().keySet().containsAll(groups.descriptors(owner).stream().map(g -> g.key()).toList())) {
                log.warn("Cache groups missing from registry (Redis cleared?), registering again");
                freshness.requireRegistryVersion(0);
                register();
            }
        } catch (RuntimeException e) {
            log.warn("Cache group registry check failed: {}", e.toString());
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
