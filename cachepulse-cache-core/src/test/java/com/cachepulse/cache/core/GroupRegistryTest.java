package com.cachepulse.cache.core;

import com.cachepulse.cache.annotation.CacheStrategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GroupRegistryTest {
    private final ObjectMapper json = new ObjectMapper();
    private final Map<Object, Object> stored = new HashMap<>();
    private StringRedisTemplate redis;
    private HashOperations<String, Object, Object> hash;
    private ValueOperations<String, String> value;
    private GroupRegistry registry;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redis = mock(StringRedisTemplate.class);
        hash = mock(HashOperations.class);
        value = mock(ValueOperations.class);
        when(redis.opsForHash()).thenReturn(hash);
        when(redis.opsForValue()).thenReturn(value);
        when(hash.entries(CacheKeys.REGISTRY_GROUPS)).thenReturn(stored);
        when(hash.putIfAbsent(eq(CacheKeys.REGISTRY_GROUPS), any(), any())).thenReturn(true);
        registry = new GroupRegistry(redis, json);
    }

    @Test
    void registersNewGroupAndBumpsVersion() {
        when(value.increment(CacheKeys.REGISTRY_VERSION)).thenReturn(3L);

        long version = registry.register(List.of(group("users", "user:#id", CacheStrategy.DELETE_MODE)));

        assertThat(version).isEqualTo(3L);
        verify(hash).putIfAbsent(eq(CacheKeys.REGISTRY_GROUPS), eq("demo_db.users"), anyString());
    }

    @Test
    void sameDefinitionFromAnotherInstanceDoesNotBumpVersion() throws Exception {
        CacheGroupDescriptor users = group("users", "user:#id", CacheStrategy.DELETE_MODE);
        stored.put("demo_db.users", json.writeValueAsString(users));
        when(value.get(CacheKeys.REGISTRY_VERSION)).thenReturn("7");

        assertThat(registry.register(List.of(users))).isEqualTo(7L);
        verify(hash, never()).putIfAbsent(anyString(), any(), any());
        verify(value, never()).increment(anyString());
    }

    @Test
    void rejectsDifferentDefinitionOfRegisteredTable() throws Exception {
        stored.put("demo_db.users", json.writeValueAsString(group("users", "user:#id", CacheStrategy.DELETE_MODE)));

        assertThatThrownBy(() -> registry.register(List.of(group("users", "member:#id", CacheStrategy.DELETE_MODE))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("different definition");
    }

    @Test
    void rejectsNonDeleteModeGroups() {
        for (CacheStrategy strategy : List.of(CacheStrategy.REFRESH_MODE, CacheStrategy.HYBRID_MODE)) {
            assertThatThrownBy(() -> registry.register(List.of(group("users", "user:#id", strategy))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("DELETE_MODE only");
        }
        verify(hash, never()).putIfAbsent(anyString(), any(), any());
    }

    @Test
    void rejectsPrefixOverlappingAnotherTable() throws Exception {
        stored.put("demo_db.users", json.writeValueAsString(group("users", "user:#id", CacheStrategy.DELETE_MODE)));

        assertThatThrownBy(() -> registry.register(List.of(group("vip_users", "user:vip:#id", CacheStrategy.DELETE_MODE))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Overlapping cache prefixes");
        verify(hash, never()).putIfAbsent(anyString(), any(), any());
    }

    @Test
    void rejectsIndexPrefixOverlappingAnotherTable() throws Exception {
        stored.put("demo_db.users", json.writeValueAsString(new CacheGroupDescriptor("demo-user-service", "demo_db",
                "users", "id", "user:#id", List.of("user-email:#email"), CacheStrategy.DELETE_MODE,
                1800, 10240, true, 0, false)));

        assertThatThrownBy(() -> registry.register(List.of(group("emails", "user-email:#id", CacheStrategy.DELETE_MODE))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Overlapping cache prefixes");
    }

    @Test
    void readsMissingVersionAsZero() {
        assertThat(registry.version()).isZero();
    }

    @Test
    void cacheKeysRejectOverlapsInEitherDirectionAndReservedNamespace() {
        assertThatThrownBy(() -> CacheKeys.requireDisjoint("user:", "user:vip:")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> CacheKeys.requireDisjoint("user:vip:", "user:")).isInstanceOf(IllegalStateException.class);
        assertThatCode(() -> CacheKeys.requireDisjoint("user:", "user-email:")).doesNotThrowAnyException();

        assertThatThrownBy(() -> CacheKeys.requireNotReserved("cachepulse:users:")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> CacheKeys.requireNotReserved("cache")).isInstanceOf(IllegalStateException.class);
        assertThatCode(() -> CacheKeys.requireNotReserved("user:")).doesNotThrowAnyException();
        assertThat(CacheKeys.bloomChannel("demo_db.users")).isEqualTo("cachepulse:bloom:demo_db.users");
    }

    private static CacheGroupDescriptor group(String table, String keyTemplate, CacheStrategy strategy) {
        return new CacheGroupDescriptor("demo-user-service", "demo_db", table, "id", keyTemplate, List.of(),
                strategy, 1800, 10240, true, 0, false);
    }
}
