package com.cachepulse.demo.user;

import com.cachepulse.cache.core.CacheFreshness;
import com.cachepulse.cache.model.AutoCacheConfig;
import com.cachepulse.cache.starter.CacheAnnotationProcessor;
import com.cachepulse.cache.starter.CacheLoaderRegistry;
import com.cachepulse.cache.starter.CacheQueryService;
import com.cachepulse.cache.starter.CachedQueryService;
import com.cachepulse.cache.starter.QueryProtection;
import com.cachepulse.demo.user.controller.UserController;
import com.cachepulse.demo.user.entity.User;
import com.cachepulse.demo.user.loader.UserCacheLoader;
import com.cachepulse.demo.user.mapper.UserMapper;
import com.cachepulse.demo.user.service.UserQueryService;
import com.cachepulse.demo.user.service.UserWriteService;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Date;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class UserQueryServiceTest {
    private final UserMapper userMapper = mock(UserMapper.class);
    private final RedisTemplate<String, String> redis = mock(RedisTemplate.class);
    private final HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
    private final CacheAnnotationProcessor processor = mock(CacheAnnotationProcessor.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private UserQueryService service;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        AutoCacheConfig config = new AutoCacheConfig();
        config.setKeyTemplate("user:#id");
        config.setExpireSeconds(1800);
        config.setEntityClass(User.class);
        when(processor.getConfig("users")).thenReturn(config);
        when(processor.getAllTables()).thenReturn(Set.of("users"));
        when(redis.opsForHash()).thenReturn(hashes);
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenAnswer(invocation -> ((RedisScript<?>) invocation.getArgument(0)).getResultType() == String.class
                        ? null : 1L);
        service = new UserQueryService(new CacheQueryService(
                new CacheLoaderRegistry(List.of(new UserCacheLoader(userMapper)), processor),
                new CachedQueryService(redis, processor, objectMapper, new SimpleMeterRegistry())));
        mvc = MockMvcBuilders.standaloneSetup(new UserController(service, mock(UserWriteService.class))).build();
    }

    @Test
    void cachedUserRetainsDateAndDoesNotQueryDatabase() throws Exception {
        User user = user();
        when(hashes.get("user:1", "data")).thenReturn(objectMapper.writeValueAsString(user));

        assertThat(service.getById(1L)).isEqualTo(user);

        verifyNoInteractions(userMapper);
    }

    @Test
    void cacheMissReturnsUserOverHttpAndFillsHashWithTtl() throws Exception {
        User user = user();
        when(userMapper.selectById(1L)).thenReturn(user);

        mvc.perform(get("/users/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.name").value("Alice"));

        verify(redis).execute(any(RedisScript.class), eq(List.of("user:1")),
                eq(objectMapper.writeValueAsString(user)), eq("1800"), anyString());
    }

    @Test
    void missingUserReturns404AndCachesNullFor30Seconds() throws Exception {
        when(hashes.get("user:99", "data")).thenReturn(null, "null");
        mvc.perform(get("/users/99")).andExpect(status().isNotFound());
        mvc.perform(get("/users/99")).andExpect(status().isNotFound());

        verify(userMapper, times(1)).selectById(99L);
        verify(redis).execute(any(RedisScript.class), eq(List.of("user:99")), eq("null"), eq("30"), anyString());
    }

    @Test
    void databaseFailureDoesNotCreateMissingUserMarker() {
        when(userMapper.selectById(99L)).thenThrow(new DataAccessResourceFailureException("MySQL unavailable"));

        assertThatThrownBy(() -> service.getById(99L))
                .isInstanceOf(DataAccessResourceFailureException.class);
        verify(redis, never()).execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString());
    }

    @Test
    void malformedCacheDataIsReportedInsteadOfHidden() {
        when(hashes.get("user:1", "data")).thenReturn("broken-json");

        assertThatThrownBy(() -> service.getById(1L))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Invalid cache data");
        verifyNoInteractions(userMapper);
    }

    @Test
    void queryUsesTheStringRedisTemplateLikeCacheSyncInvalidation() {
        // 删缓存在 cache-sync-service（VersionControllerWithLua），那里同样用 StringRedisTemplate；这里只验证查询侧。
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RedisAutoConfiguration.class))
                .withBean(RedisConnectionFactory.class, () -> mock(RedisConnectionFactory.class))
                .withBean(UserMapper.class, () -> userMapper)
                .withBean(CacheAnnotationProcessor.class, () -> processor)
                .withBean(ObjectMapper.class, () -> objectMapper)
                .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .withBean(CacheFreshness.class, () -> CacheFreshness.ALWAYS)
                .withBean(QueryProtection.class, QueryProtection::disabled)
                .withBean(CachedQueryService.class)
                .withBean(UserCacheLoader.class)
                .withBean(CacheLoaderRegistry.class)
                .withBean(CacheQueryService.class)
                .withBean(UserQueryService.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    StringRedisTemplate strings = context.getBean(StringRedisTemplate.class);
                    assertThat(ReflectionTestUtils.getField(context.getBean(CachedQueryService.class), "redis"))
                            .isSameAs(strings);
                    assertThat(strings.getValueSerializer()).isSameAs(RedisSerializer.string());
                });
    }

    private User user() {
        User user = new User();
        user.setId(1L);
        user.setName("Alice");
        user.setCreatedAt(new Date(1700000000000L));
        return user;
    }
}
