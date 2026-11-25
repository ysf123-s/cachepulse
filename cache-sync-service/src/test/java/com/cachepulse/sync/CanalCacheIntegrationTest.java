package com.cachepulse.sync;

import com.cachepulse.cache.core.GroupRegistry;
import com.cachepulse.common.kafka.CanalConnectorWrapper;
import com.cachepulse.common.model.BinlogEvent;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * 集成测试：分组写进真实 Redis 的注册表，同步服务加载后，BinlogEvent 触发正确的 Redis 缓存操作。
 * 使用 Testcontainers 拉起真实 Redis，CanalConnectorWrapper mock 掉防止连 Kafka；不连 Nacos。
 * 接入组件只是测试依赖，它的自动配置排除掉，同步服务只认注册表里的分组。
 */
@SpringBootTest(properties = {
        "cachepulse.config-prefix=optional:file:./config/nacos/",
        "spring.cloud.nacos.discovery.enabled=false",
        "spring.cloud.nacos.config.enabled=false",
        "spring.autoconfigure.exclude=com.cachepulse.cache.starter.CachePulseCacheAutoConfiguration",
        "canal.heartbeat.enabled=false"})
@Testcontainers(disabledWithoutDocker = true)
class CanalCacheIntegrationTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        // 测试容器未启用密码；显式清空，避免继承环境变量里的密码。
        registry.add("spring.data.redis.password", () -> "");
    }

    // mock 掉 Kafka 连接，防止 @PostConstruct 真正连接
    @MockitoBean
    private CanalConnectorWrapper canalConnectorWrapper;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private CanalCacheSyncService syncService;

    @Autowired
    private GroupRegistry registry;

    @Autowired
    private RegistryCacheGroups groups;

    @BeforeEach
    void setUp() {
        // 同步循环走 Entry 模式，测试用例直接调用 handleEvent，不真正连接 Kafka。
        when(canalConnectorWrapper.isFlatMessageMode()).thenReturn(false);
        long version = registry.register(List.of(
                MultiTableCacheTest.group("users", "user:#id", 1800, "user-email:#email"),
                MultiTableCacheTest.group("departments", "department:#id", 600)));
        groups.refresh();
        assertThat(groups.loadedVersion()).isEqualTo(version);

        // 清理测试 key 保证用例隔离
        redisTemplate.delete(List.of("user:1", "user:2", "user:99", "department:1"));
    }

    @Test
    void deleteEvent_shouldRemoveCacheKey() {
        Map<String, String> hash = new HashMap<>();
        hash.put("data", "{\"id\":1,\"name\":\"test\"}");
        hash.put("version", "0");
        redisTemplate.opsForHash().putAll("user:1", hash);
        assertThat(redisTemplate.hasKey("user:1")).isTrue();

        syncService.handleEvent(BinlogEvent.builder()
                .databaseName("demo_db")
                .tableName("users")
                .eventType(EventType.DELETE)
                .beforeData(Map.of("id", "1", "name", "test"))
                .binlogPosition(100L)
                .build());

        assertThat(redisTemplate.hasKey("user:1")).isFalse();
    }

    @Test
    void updateEvent_deleteStrategy_shouldInvalidateExistingCacheKey() {
        // 跨进程的分组只有 DELETE_MODE：更新数据库后删除缓存，下一次查询负责回填。
        redisTemplate.opsForHash().putAll("user:2", Map.of("data", "{\"id\":2,\"name\":\"old\"}", "version", "0"));

        syncService.handleEvent(BinlogEvent.builder()
                .databaseName("demo_db")
                .tableName("users")
                .eventType(EventType.UPDATE)
                .beforeData(Map.of("id", "2", "name", "old"))
                .afterData(Map.of("id", "2", "name", "new"))
                .binlogPosition(200L)
                .build());

        assertThat(redisTemplate.hasKey("user:2")).isFalse();
    }

    @Test
    void deleteEvent_keyNotInRedis_shouldBeIdempotent() {
        syncService.handleEvent(BinlogEvent.builder()
                .databaseName("demo_db")
                .tableName("users")
                .eventType(EventType.DELETE)
                .beforeData(Map.of("id", "99"))
                .binlogPosition(300L)
                .build());
        assertThat(redisTemplate.hasKey("user:99")).isFalse();
    }

    @Test
    void unknownTable_shouldBeIgnoredSilently() {
        // 注册表里没有的表，事件被忽略，不报错
        syncService.handleEvent(BinlogEvent.builder()
                .databaseName("demo_db")
                .tableName("unknown_table")
                .eventType(EventType.DELETE)
                .beforeData(Map.of("id", "1"))
                .binlogPosition(400L)
                .build());
    }

    @Test
    void departmentUsesSameInvalidationFlow() {
        redisTemplate.opsForHash().putAll("department:1", Map.of("data", "{\"id\":1,\"name\":\"R&D\"}", "version", "0"));
        syncService.handleEvent(BinlogEvent.builder().databaseName("demo_db").tableName("departments")
                .eventType(EventType.UPDATE).afterData(Map.of("id", "1", "name", "updated")).build());
        assertThat(redisTemplate.hasKey("department:1")).isFalse();
    }
}
