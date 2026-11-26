package com.cachepulse.sync;

import com.cachepulse.cache.core.GroupRegistry;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class RegistryCacheGroupsTest {
    private final GroupRegistry registry = mock(GroupRegistry.class);
    private final RegistryCacheGroups groups = new RegistryCacheGroups(registry);

    @Test
    void picksUpGroupsRegisteredUnderANewVersion() {
        when(registry.version()).thenReturn(1L);
        when(registry.loadAll()).thenReturn(Map.of("demo_db.users", MultiTableCacheTest.group("users", "user:#id", 1800)));
        assertThat(groups.loadedVersion()).isEqualTo(-1);

        groups.refresh();
        groups.refresh();

        assertThat(groups.loadedVersion()).isEqualTo(1);
        assertThat(groups.getAllTables()).containsExactly("users");
        assertThat(groups.getConfig("departments")).isNull();
        verify(registry, times(1)).loadAll();

        when(registry.version()).thenReturn(2L);
        when(registry.loadAll()).thenReturn(Map.of(
                "demo_db.users", MultiTableCacheTest.group("users", "user:#id", 1800),
                "demo_db.departments", MultiTableCacheTest.group("departments", "department:#id", 600)));

        groups.refresh();

        assertThat(groups.loadedVersion()).isEqualTo(2);
        assertThat(groups.getAllTables()).containsExactlyInAnyOrder("users", "departments");
        assertThat(groups.getConfig("departments").getDatabase()).isEqualTo("demo_db");
        assertThat(groups.getConfig("departments").buildCacheKeySimple(7)).isEqualTo("department:7");
    }

    @Test
    void sameTableNameInTwoDatabasesStaysTwoGroups() {
        var demo = MultiTableCacheTest.group("users", "user:#id", 1800);
        var stock = new com.cachepulse.cache.core.CacheGroupDescriptor("stock-service", "stock_db", "users", "id",
                "stock-user:#id", java.util.List.of(), com.cachepulse.cache.annotation.CacheStrategy.DELETE_MODE,
                600, 10240, false, 10, false);
        when(registry.version()).thenReturn(1L);
        when(registry.loadAll()).thenReturn(Map.of("demo_db.users", demo, "stock_db.users", stock));

        groups.refresh();

        assertThat(groups.getConfig("demo_db", "users").buildCacheKeySimple(1)).isEqualTo("user:1");
        assertThat(groups.getConfig("stock_db", "users").buildCacheKeySimple(1)).isEqualTo("stock-user:1");
        assertThat(groups.getConfig("other_db", "users")).isNull();
        assertThat(groups.getConfig(null, "users")).as("ambiguous without a database").isNull();
        assertThat(groups.getAllTables()).containsExactlyInAnyOrder("demo_db.users", "stock_db.users");
        assertThat(groups.getConfig("stock_db.users").buildCacheKeySimple(2)).isEqualTo("stock-user:2");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> groups.getConfig("users"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("database.table");
    }

    @Test
    void uniqueTableNameWorksWithOrWithoutTheDatabase() {
        when(registry.version()).thenReturn(1L);
        when(registry.loadAll()).thenReturn(Map.of("demo_db.users", MultiTableCacheTest.group("users", "user:#id", 1800)));

        groups.refresh();

        assertThat(groups.getConfig("users")).isSameAs(groups.getConfig("demo_db", "users"));
        assertThat(groups.getConfig("demo_db.users")).isSameAs(groups.getConfig("demo_db", "users"));
        assertThat(groups.getConfig(null, "users")).isSameAs(groups.getConfig("demo_db", "users"));
        assertThat(groups.getConfig("other_db", "users")).isNull();
    }

    @Test
    void notLoadedUntilTheRegistryHasBeenReadOnce() {
        assertThat(groups.isLoaded()).isFalse();

        when(registry.version()).thenThrow(new RedisConnectionFailureException("down"));
        groups.refresh();
        assertThat(groups.isLoaded()).as("a failed first read leaves it unloaded").isFalse();

        reset(registry);
        when(registry.version()).thenReturn(0L);
        when(registry.loadAll()).thenReturn(Map.of());
        groups.refresh();
        assertThat(groups.isLoaded()).as("an empty registry still counts as read").isTrue();
    }

    @Test
    void registryFailureKeepsTheLastLoadedGroupsAndVersion() {
        when(registry.version()).thenReturn(1L);
        when(registry.loadAll()).thenReturn(Map.of("demo_db.users", MultiTableCacheTest.group("users", "user:#id", 1800)));
        groups.refresh();

        when(registry.version()).thenReturn(2L);
        when(registry.loadAll()).thenThrow(new RedisConnectionFailureException("down"));
        groups.refresh();

        assertThat(groups.loadedVersion()).isEqualTo(1);
        assertThat(groups.getAllTables()).containsExactly("users");
    }
}
