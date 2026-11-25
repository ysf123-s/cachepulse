package com.cachepulse.cache.starter;

import com.cachepulse.cache.model.AutoCacheConfig;
import com.cachepulse.cache.starter.fixture.User;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PrimaryKeyFiltersTest {
    private final CacheAnnotationProcessor processor = mock(CacheAnnotationProcessor.class);
    private final AutoCacheConfig config = new AutoCacheConfig();

    @BeforeEach
    void setUp() {
        config.setTable("users");
        config.setEntityClass(User.class);
        config.setBloomFilter(true);
        when(processor.getConfig("users")).thenReturn(config);
        when(processor.getAllTables()).thenReturn(Set.of("users"));
    }

    @Test
    void bloomFilterHasNoFalseNegativesAndStaysNearItsFalsePositiveRate() {
        PrimaryKeyBloomFilter filter = new PrimaryKeyBloomFilter(10_000, 0.01);
        for (long id = 1; id <= 10_000; id++) {
            filter.add(id);
        }

        assertThat(LongStream.rangeClosed(1, 10_000).allMatch(filter::mightContain)).isTrue();
        long falsePositives = LongStream.rangeClosed(1_000_001, 1_100_000).filter(filter::mightContain).count();
        assertThat(falsePositives / 100_000.0).isLessThan(0.02);
        assertThat(filter.approximateCount()).isBetween(9_900L, 10_000L);
    }

    @Test
    void untilBuiltEveryIdCountsAsPossiblyPresent() {
        PrimaryKeyFilters filters = filters((after, limit) -> List.of());

        assertThat(filters.mightContain("users", 12345)).isTrue();
        assertThat(filters.status().get("users").ready()).isFalse();
    }

    @Test
    void buildReadsEveryPageAndKeepsIdsAddedByCdcMeanwhile() {
        List<Long> afters = new ArrayList<>();
        PrimaryKeyFilters filters = filters((after, limit) -> {
            afters.add(after);
            return LongStream.rangeClosed(1, 5).filter(id -> id > after).limit(limit).boxed().toList();
        });
        filters.add("users", "42");

        filters.buildAll();

        assertThat(afters).containsExactly(Long.MIN_VALUE, 2L, 4L);
        assertThat(LongStream.of(1, 2, 3, 4, 5, 42).allMatch(id -> filters.mightContain("users", id))).isTrue();
        long falsePositives = LongStream.rangeClosed(1_000, 1_099).filter(id -> filters.mightContain("users", id)).count();
        assertThat(falsePositives).isLessThan(5);
        assertThat(filters.status().get("users").ready()).isTrue();
        assertThat(filters.status().get("users").approximateCount()).isEqualTo(6);
        assertThat(filters.mightContain("orders", 1)).isTrue();
    }

    @Test
    void transientDatabaseFailureIsRetriedUntilTheFilterIsBuilt() {
        AtomicInteger calls = new AtomicInteger();
        PrimaryKeyFilters filters = filters((after, limit) -> {
            if (calls.incrementAndGet() == 1) {
                throw new DataAccessResourceFailureException("MySQL unavailable");
            }
            return List.of();
        });

        filters.buildAll();

        assertThat(calls).hasValue(2);
        assertThat(filters.status().get("users").ready()).isTrue();
        assertThat(filters.status().get("users").error()).isNull();
        assertThat(filters.mightContain("users", 7)).isFalse();
    }

    @Test
    void loaderWithoutPrimaryKeyScanLeavesTheTableUnfiltered() {
        PrimaryKeyFilters filters = new PrimaryKeyFilters(processor,
                new CacheLoaderRegistry(List.of(loader(null)), processor), true, 1_000, 0.01, 2, 1);

        filters.buildAll();

        assertThat(filters.status().get("users").ready()).isFalse();
        assertThat(filters.status().get("users").error()).contains("No primary key scan");
        assertThat(filters.mightContain("users", 7)).isTrue();
    }

    @Test
    void disabledFiltersAndNonNumericIdsAreIgnored() {
        PrimaryKeyFilters disabled = new PrimaryKeyFilters(processor,
                new CacheLoaderRegistry(List.of(loader((after, limit) -> List.of())), processor), false, 1_000, 0.01, 2, 1);
        disabled.add("users", 1L);
        assertThat(disabled.status()).isEmpty();
        assertThat(disabled.mightContain("users", 1)).isTrue();

        PrimaryKeyFilters filters = filters((after, limit) -> List.of());
        filters.add("users", "not-a-number");
        filters.buildAll();
        assertThat(filters.status().get("users").approximateCount()).isZero();
    }

    private PrimaryKeyFilters filters(BiFunction<Long, Integer, List<Long>> scan) {
        return new PrimaryKeyFilters(processor, new CacheLoaderRegistry(List.of(loader(scan)), processor),
                true, 1_000, 0.01, 2, 1);
    }

    private static CacheDataLoader<User> loader(BiFunction<Long, Integer, List<Long>> scan) {
        return new CacheDataLoader<>() {
            @Override public String table() { return "users"; }
            @Override public Class<User> entityType() { return User.class; }
            @Override public User loadById(Long id) { return null; }
            @Override public List<User> loadByIds(List<Long> ids) { return List.of(); }
            @Override public Long idOf(User entity) { return entity.getId(); }
            @Override public List<Long> loadIdsAfter(long afterId, int limit) {
                return scan == null ? CacheDataLoader.super.loadIdsAfter(afterId, limit) : scan.apply(afterId, limit);
            }
        };
    }
}
