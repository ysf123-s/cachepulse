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
