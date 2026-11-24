package com.example.canalcache.service;

import com.example.canalcache.entity.User;
import com.example.canalcache.loader.CacheDataLoader;
import com.example.canalcache.model.AutoCacheConfig;
import com.example.canalcache.model.BatchQueryResult;
import com.example.canalcache.service.CacheLoadGuard.Lane;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class QueryProtectionTest {
    private final RedisTemplate<String, String> redis = mock(RedisTemplate.class);
    private final HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
    private final CacheAnnotationProcessor processor = mock(CacheAnnotationProcessor.class);
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final ObjectMapper json = new ObjectMapper();
    private final AtomicLong now = new AtomicLong(1_000_000);
    private final AutoCacheConfig config = new AutoCacheConfig();

    @BeforeEach
    void setUp() {
        config.setTable("users");
        config.setKeyTemplate("user:#id");
        config.setExpireSeconds(1800);
        config.setIdField("id");
        config.setEntityClass(User.class);
        config.setIdFieldType(Long.class);
        when(processor.getConfig("users")).thenReturn(config);
        when(processor.getAllTables()).thenReturn(Set.of("users"));
        when(redis.opsForHash()).thenReturn(hashes);
        when(redis.getStringSerializer()).thenReturn(RedisSerializer.string());
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenAnswer(invocation -> ((RedisScript<?>) invocation.getArgument(0)).getResultType() == String.class
                        ? null : 1L);
    }

    @Test
    void redisReadFailureFallsBackToTheDatabaseWithoutRefilling() {
        RedisCircuitBreaker breaker = breaker(5);
        when(hashes.get("user:1", "data")).thenThrow(new RedisConnectionFailureException("redis offline"));

        User result = service(breaker, CacheLoadGuard.unlimited(), PrimaryKeyFilters.none())
                .getById("users", 1L, User.class, () -> user(1));

        assertThat(result.getId()).isEqualTo(1);
        verify(redis, never()).execute(any(RedisScript.class), anyList(), any(Object[].class));
        assertThat(metrics.get("cache.query.items").tag("result", "redis_fallback").counter().count()).isEqualTo(1);
        assertThat(breaker.status().state()).isEqualTo(RedisCircuitBreaker.State.CLOSED);
        assertThat(breaker.status().consecutiveFailures()).isEqualTo(1);
        assertThat(breaker.status().fallbacks()).isEqualTo(1);
    }

    @Test
    void breakerSkipsRedisAfterConsecutiveFailuresAndProbesOnceTheOpenPeriodEnds() throws Exception {
        RedisCircuitBreaker breaker = breaker(2);
        CachedQueryService service = service(breaker, CacheLoadGuard.unlimited(), PrimaryKeyFilters.none());
        when(hashes.get("user:1", "data")).thenThrow(new RedisConnectionFailureException("redis offline"));
        service.getById("users", 1L, User.class, () -> user(1));
        service.getById("users", 1L, User.class, () -> user(1));
        assertThat(breaker.status().state()).isEqualTo(RedisCircuitBreaker.State.OPEN);

        assertThat(service.getById("users", 1L, User.class, () -> user(1)).getId()).isEqualTo(1);
        verify(hashes, times(2)).get("user:1", "data");

        now.addAndGet(5_000);
        reset(hashes);
        when(hashes.get("user:1", "data")).thenReturn(json.writeValueAsString(user(1)));
        User probed = service.getById("users", 1L, User.class, () -> {
            throw new AssertionError("the probe should hit the cache");
        });

        assertThat(probed.getId()).isEqualTo(1);
        assertThat(breaker.status().state()).isEqualTo(RedisCircuitBreaker.State.CLOSED);
        assertThat(breaker.status().fallbacks()).isEqualTo(3);
    }

    @Test
    void failedLoadRegistrationStillReturnsTheDatabaseResultWithoutRefillOrCleanup() {
        RedisCircuitBreaker breaker = breaker(5);
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString()))
                .thenThrow(new RedisConnectionFailureException("redis offline"));

        User result = service(breaker, CacheLoadGuard.unlimited(), PrimaryKeyFilters.none())
                .getById("users", 1L, User.class, () -> user(1));

        assertThat(result.getId()).isEqualTo(1);
        verify(redis, never()).execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString());
        verify(redis, never()).execute(any(RedisScript.class), anyList(), anyString());
        assertThat(breaker.status().consecutiveFailures()).isEqualTo(1);
    }

    @Test
    void failedRefillOnlySkipsCachingAndTheNextSuccessResetsTheBreaker() {
        RedisCircuitBreaker breaker = breaker(5);
        CachedQueryService service = service(breaker, CacheLoadGuard.unlimited(), PrimaryKeyFilters.none());
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString()))
                .thenThrow(new RedisConnectionFailureException("redis offline")).thenReturn(1L);

        assertThat(service.getById("users", 1L, User.class, () -> user(1)).getId()).isEqualTo(1);
        assertThat(breaker.status().consecutiveFailures()).isEqualTo(1);
        verify(redis, never()).execute(any(RedisScript.class), anyList(), anyString());

        assertThat(service.getById("users", 2L, User.class, () -> user(2)).getId()).isEqualTo(2);
        assertThat(breaker.status().consecutiveFailures()).isZero();
    }

    @Test
    void indexLookupFallsBackToTheDatabaseWhenRedisFails() {
        config.setIndexKeys(List.of(AutoCacheConfig.IndexKey.parse("user-email:#email")));
        RedisCircuitBreaker breaker = breaker(5);
        when(hashes.get("user-email:a@x.com", "data")).thenThrow(new RedisConnectionFailureException("redis offline"));

        List<Long> ids = service(breaker, CacheLoadGuard.unlimited(), PrimaryKeyFilters.none())
                .getIndexedIds("users", "email", "a@x.com", () -> List.of(5L, 1L));

        assertThat(ids).containsExactly(1L, 5L);
        verify(redis, never()).execute(any(RedisScript.class), anyList(), any(Object[].class));
        assertThat(metrics.get("cache.index.items").tag("result", "redis_fallback").counter().count()).isEqualTo(1);
    }

    @Test
    void withoutABreakerRedisFailuresStillPropagate() {
        when(hashes.get("user:1", "data")).thenThrow(new RedisConnectionFailureException("redis offline"));

        assertThatThrownBy(() -> new CachedQueryService(redis, processor, json, metrics)
                .getById("users", 1L, User.class, () -> user(1)))
                .isInstanceOf(RedisConnectionFailureException.class);
    }

    @Test
    void loadLimitRejectsExtraLoadsButSameKeyRequestsShareTheRunningOne() throws Exception {
        CacheLoadGuard guard = new CacheLoadGuard(1, 0, 1, 0);
        CachedQueryService service = service(breaker(5), guard, PrimaryKeyFilters.none());
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(2);
        try {
            Future<User> holder = pool.submit(() -> service.getById("users", 1L, User.class, () -> {
                loading.countDown();
                awaitLatch(release);
                return user(1);
            }));
            assertThat(loading.await(5, TimeUnit.SECONDS)).isTrue();
            Future<User> sharing = pool.submit(() -> service.getById("users", 1L, User.class, () -> user(1)));
            await().atMost(Duration.ofSeconds(5)).until(() -> coalesced() == 1);

            assertThatThrownBy(() -> service.getById("users", 2L, User.class, () -> user(2)))
                    .isInstanceOf(CacheLoadRejectedException.class);
            assertThat(guard.status().get(Lane.NORMAL).rejected()).isEqualTo(1);

            release.countDown();
            assertThat(holder.get(5, TimeUnit.SECONDS).getId()).isEqualTo(1);
            assertThat(sharing.get(5, TimeUnit.SECONDS).getId()).isEqualTo(1);
            assertThat(service.getById("users", 2L, User.class, () -> user(2)).getId()).isEqualTo(2);
            assertThat(guard.status().get(Lane.NORMAL).inUse()).isZero();
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
        assertThat(CacheLoadRejectedException.class.getAnnotation(ResponseStatus.class).value())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Test
    void requestsSharingARejectedLoadAreRejectedToo() throws Exception {
        CacheLoadGuard guard = new CacheLoadGuard(1, 1_000, 1, 0);
        CachedQueryService service = service(breaker(5), guard, PrimaryKeyFilters.none());
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(3);
        try {
            pool.submit(() -> service.getById("users", 1L, User.class, () -> {
                loading.countDown();
                awaitLatch(release);
                return user(1);
            }));
            assertThat(loading.await(5, TimeUnit.SECONDS)).isTrue();
            Future<User> first = pool.submit(() -> service.getById("users", 2L, User.class, () -> user(2)));
            Future<User> second = pool.submit(() -> service.getById("users", 2L, User.class, () -> user(2)));
            await().atMost(Duration.ofSeconds(5)).until(() -> coalesced() == 1);

            for (Future<User> future : List.of(first, second)) {
                assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(CacheLoadRejectedException.class);
            }
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void idsTheBloomFilterRulesOutUseTheNarrowLaneAndCannotStarveRealLookups() throws Exception {
        CacheLoadGuard guard = new CacheLoadGuard(1, 0, 1, 0);
        CachedQueryService service = service(breaker(5), guard, readyFilters(1L, 2L, 3L));
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(1);
        try {
            Future<User> suspect = pool.submit(() -> service.getById("users", 900L, User.class, () -> {
                loading.countDown();
                awaitLatch(release);
                return null;
            }));
            assertThat(loading.await(5, TimeUnit.SECONDS)).isTrue();

            assertThatThrownBy(() -> service.getById("users", 901L, User.class, () -> null))
                    .isInstanceOfSatisfying(CacheLoadRejectedException.class,
                            rejected -> assertThat(rejected.lane()).isEqualTo(Lane.SUSPECT));
            assertThat(service.getById("users", 2L, User.class, () -> user(2)).getId()).isEqualTo(2);

            release.countDown();
            assertThat(suspect.get(5, TimeUnit.SECONDS)).isNull();
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void mixedBatchLoadsLikelyIdsFirstAndSuspectIdsInASeparateQuery() {
        when(redis.executePipelined(any(RedisCallback.class), any(RedisSerializer.class)))
                .thenReturn(Arrays.asList(null, null), Arrays.asList(null, null));
        List<List<Long>> calls = new ArrayList<>();

        BatchQueryResult<User> result = service(breaker(5), CacheLoadGuard.unlimited(), readyFilters(3L))
                .getByIds("users", List.of(1001L, 3L), User.class, ids -> {
                    calls.add(ids);
                    return ids.contains(3L) ? List.of(user(3)) : List.of();
                }, User::getId);

        assertThat(calls).containsExactly(List.of(3L), List.of(1001L));
        assertThat(result.items()).extracting(User::getId).containsExactly(3L);
        assertThat(result.missingIds()).containsExactly(1001L);
    }

    @Test
    void ttlJitterSpreadsExpiryWithinTheConfiguredPercent() {
        config.setTtlJitterPercent(10);
        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            int ttl = config.jitteredSeconds(1800);
            assertThat(ttl).isBetween(1800, 1980);
            seen.add(ttl);
        }
        assertThat(seen).hasSizeGreaterThan(20);
        assertThat(config.jitteredSeconds(30)).isBetween(30, 33);

        config.setTtlJitterPercent(0);
        assertThat(config.jitteredSeconds(1800)).isEqualTo(1800);
    }

    @Test
    void refillsUseTheJitteredTtl() {
        config.setTtlJitterPercent(10);
        List<Integer> ttls = Collections.synchronizedList(new ArrayList<>());
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString()))
                .thenAnswer(invocation -> {
                    ttls.add(Integer.parseInt(invocation.getArgument(3)));
                    return 1L;
                });
        CachedQueryService service = service(breaker(5), CacheLoadGuard.unlimited(), PrimaryKeyFilters.none());

        for (long id = 1; id <= 50; id++) {
            long current = id;
            service.getById("users", current, User.class, () -> user(current));
        }

        assertThat(ttls).hasSize(50).allSatisfy(ttl -> assertThat(ttl).isBetween(1800, 1980));
        assertThat(new HashSet<>(ttls)).hasSizeGreaterThan(1);
    }

    private RedisCircuitBreaker breaker(int threshold) {
        return new RedisCircuitBreaker(threshold, 5_000, now::get);
    }

    private CachedQueryService service(RedisCircuitBreaker breaker, CacheLoadGuard guard, PrimaryKeyFilters filters) {
        return new CachedQueryService(redis, processor, json, metrics, CacheFreshness.ALWAYS,
                new QueryProtection(breaker, guard, filters));
    }

    private PrimaryKeyFilters readyFilters(Long... ids) {
        config.setBloomFilter(true);
        CacheDataLoader<User> loader = new CacheDataLoader<>() {
            @Override public String table() { return "users"; }
            @Override public Class<User> entityType() { return User.class; }
            @Override public User loadById(Long id) { return null; }
            @Override public List<User> loadByIds(List<Long> requested) { return List.of(); }
            @Override public Long idOf(User entity) { return entity.getId(); }
            @Override public List<Long> loadIdsAfter(long afterId, int limit) {
                return Arrays.stream(ids).filter(id -> id > afterId).sorted().limit(limit).toList();
            }
        };
        PrimaryKeyFilters filters = new PrimaryKeyFilters(processor,
                new CacheLoaderRegistry(List.of(loader), processor), true, 1_000, 0.01, 100, 1);
        filters.buildAll();
        return filters;
    }

    private double coalesced() {
        var counter = metrics.find("cache.query.coalesced").counter();
        return counter == null ? 0 : counter.count();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static User user(long id) {
        User user = new User();
        user.setId(id);
        user.setName("User-" + id);
        return user;
    }
}
