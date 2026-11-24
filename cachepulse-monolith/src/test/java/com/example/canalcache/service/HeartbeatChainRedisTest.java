package com.example.canalcache.service;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.model.BinlogEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Opt-in real Redis test for the heartbeat chain script. Uses a unique key and deletes it afterwards. */
@EnabledIfSystemProperty(named = "cache.test.redis.host", matches = ".+")
class HeartbeatChainRedisTest {
    private final AtomicLong now = new AtomicLong(1_000_000);
    private final String key = "canal-cache:test-heartbeat-chain:" + UUID.randomUUID();
    private LettuceConnectionFactory factory;
    private StringRedisTemplate redis;

    @BeforeEach
    void setUp() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(System.getProperty("cache.test.redis.host"),
                Integer.parseInt(System.getenv().getOrDefault("CACHE_TEST_REDIS_PORT", "6379")));
        config.setDatabase(15);
        config.setPassword(RedisPassword.of(System.getenv().getOrDefault("CACHE_TEST_REDIS_PASSWORD", "")));
        factory = new LettuceConnectionFactory(config);
        factory.afterPropertiesSet();
        factory.start();
        redis = new StringRedisTemplate(factory);
    }

    @AfterEach
    void tearDown() {
        redis.delete(key);
        factory.destroy();
    }

    @Test
    void contiguousUpdatesAndReplaysAreSilentWhileSkippedUpdatesAreReported() {
        CdcHeartbeatMonitor monitor = monitor();

        monitor.checkContinuity(List.of(insert("1", 100), insert("2", 100)));
        monitor.checkContinuity(List.of(update("1", 100, 200), update("2", 100, 200)));
        monitor.checkContinuity(List.of(update("1", 100, 200)));
        assertThat(monitor.status().lossEvents()).isZero();
        assertThat(redis.opsForHash().entries(key)).containsEntry("1", "200").containsEntry("2", "200");

        monitor.checkContinuity(List.of(update("1", 300, 400), update("2", 200, 300)));

        assertThat(monitor.status().lossEvents()).isEqualTo(1);
        assertThat(monitor.status().lastLoss().heartbeatRows()).isEqualTo(1);
        assertThat(monitor.status().lastLoss().fromMs()).isEqualTo(200);
        assertThat(monitor.status().lastLoss().toMs()).isEqualTo(400);
        assertThat(redis.opsForHash().entries(key)).containsEntry("1", "400").containsEntry("2", "300");
    }

    @Test
    void baselineSurvivesRestartsAndADeletedRowStartsOver() {
        monitor().checkContinuity(List.of(insert("1", 100), update("1", 100, 200), insert("2", 100)));

        CdcHeartbeatMonitor restarted = monitor();
        restarted.checkContinuity(List.of(update("1", 200, 300)));
        assertThat(restarted.status().lossEvents()).isZero();

        restarted.checkContinuity(List.of(delete("2", 100), update("2", 500, 600)));
        assertThat(restarted.status().lossEvents()).isZero();

        restarted.checkContinuity(List.of(update("1", 900, 1000)));
        assertThat(restarted.status().lossEvents()).isEqualTo(1);
        assertThat(restarted.status().lastLoss().fromMs()).isEqualTo(300);
    }

    private CdcHeartbeatMonitor monitor() {
        return new CdcHeartbeatMonitor(true, "cdc_heartbeat", 2, 1_000, 3_000, 1, mock(JdbcTemplate.class),
                now::get, null, redis, key);
    }

    private static BinlogEvent insert(String row, long after) {
        return event(EventType.INSERT, null, image(row, after));
    }

    private static BinlogEvent update(String row, long before, long after) {
        return event(EventType.UPDATE, image(row, before), image(row, after));
    }

    private static BinlogEvent delete(String row, long before) {
        return event(EventType.DELETE, image(row, before), null);
    }

    private static Map<String, Object> image(String row, long beat) {
        Map<String, Object> image = new HashMap<>();
        image.put("id", row);
        image.put("beat_ms", String.valueOf(beat));
        return image;
    }

    private static BinlogEvent event(EventType type, Map<String, Object> before, Map<String, Object> after) {
        return BinlogEvent.builder().tableName("cdc_heartbeat").eventType(type).kafkaPartition(0)
                .beforeData(before).afterData(after).build();
    }
}
