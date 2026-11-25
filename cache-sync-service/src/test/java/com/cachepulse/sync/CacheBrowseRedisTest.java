package com.cachepulse.sync;

import com.cachepulse.cache.model.AutoCacheConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

@EnabledIfSystemProperty(named = "cache.test.redis.host", matches = ".+")
class CacheBrowseRedisTest {
    private LettuceConnectionFactory factory;
    private StringRedisTemplate redis;
    private AutoCacheConfig users;
    private String prefix;
    private final List<String> ownedKeys = new ArrayList<>();
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();


    @BeforeEach
    void setUp() {
        factory = CacheFillRedisTest.connectionFactory(System.getProperty("cache.test.redis.host"));
        redis = new StringRedisTemplate(factory);
        prefix = "browse-test:" + UUID.randomUUID() + ":";
        users = CacheFillRedisTest.configuration(prefix + "user:#id");
    }

    @AfterEach
    void tearDown() {
        redis.delete(ownedKeys); factory.destroy(); metrics.close();
    }

    @Test
    void realCursorWalkKeepsGroupAndExactIdsWithoutLosingBatchItems() {
        Set<String> expected = new HashSet<>();
        for (long id = 1; id <= 130; id++) {
            String key = own("user:" + id); expected.add(key);
            redis.opsForHash().put(key, "data", "null");
        }
        String max = own("user:" + Long.MAX_VALUE); expected.add(max);
        redis.opsForHash().put(max, "data", "null");
        String loading = own("user:201"); expected.add(loading);
        redis.opsForHash().put(loading, "load_token", "pending");
        redis.expire(loading, java.time.Duration.ofSeconds(30));
        for (String suffix : List.of("stats", "01", "0", "-1", "9223372036854775808"))
            redis.opsForHash().put(own("user:" + suffix), "data", "metadata");
        redis.opsForHash().put(own("department:1"), "data", "other-group");
        redis.opsForValue().set(own("user:999"), "wrong-type");

        CacheBrowseService browser = new CacheBrowseService(redis);
        Set<String> actual = new HashSet<>();
        String cursor = "0";
        int requests = 0;
        do {
            var page = browser.browse("users", users, cursor, 1);
            page.entries().forEach(entry -> {
                assertThat(entry.key()).isEqualTo(prefix + "user:" + entry.id());
                actual.add(entry.key());
            });
            cursor = page.nextCursor();
            assertThat(++requests).as("SCAN must eventually terminate").isLessThan(1000);
        } while (!cursor.equals("0"));
        assertThat(requests).isGreaterThan(1);
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void purgeRemovesCachedAndLoadingEntriesButKeepsMetadataAndOtherGroups() {
        for (long id = 1; id <= 30; id++) redis.opsForHash().put(own("user:" + id), "data", "null");
        String loading = own("user:201");
        redis.opsForHash().put(loading, "load_token", "pending");
        String metadata = own("user:stats");
        redis.opsForHash().put(metadata, "data", "metadata");
        String other = own("department:1");
        redis.opsForHash().put(other, "data", "other-group");

        long deleted = new CacheNamespacePurger(redis, new CacheBrowseService(redis))
                .purge("users", users);

        assertThat(deleted).isEqualTo(31);
        assertThat(redis.hasKey(prefix + "user:1")).isFalse();
        assertThat(redis.hasKey(loading)).isFalse();
        assertThat(redis.hasKey(metadata)).isTrue();
        assertThat(redis.hasKey(other)).isTrue();
    }

    @Test
    void deadLettersRoundTripThroughRealStreamNewestFirst() {
        DeadLetterStore store = new DeadLetterStore(redis, new ObjectMapper(), metrics, own("dead-letters"));
        store.recordAll(List.of(letter("first"), letter("second")));

        List<DeadLetterStore.Entry> recent = store.recent(10);

        assertThat(recent).extracting(entry -> entry.fields().get("message")).containsExactly("second", "first");
        assertThat(recent.get(0).fields()).containsEntry("reason", "MISSING_PRIMARY_KEY")
                .containsEntry("table", "users").containsEntry("after", "{\"name\":\"x\"}")
                .containsKey("recordedAt").doesNotContainKey("before");
        assertThat(metrics.get(DeadLetterStore.METRIC).tag("reason", "MISSING_PRIMARY_KEY").counter().count())
                .isEqualTo(2);
    }

    private static DeadLetterStore.DeadLetter letter(String message) {
        return new DeadLetterStore.DeadLetter("MISSING_PRIMARY_KEY", message, "users", "UPDATE", 0, 1L,
                "mysql-bin.000001", 100L, null, Map.of("name", "x"));
    }

    private String own(String suffix) { String key = prefix + suffix; ownedKeys.add(key); return key; }
}
