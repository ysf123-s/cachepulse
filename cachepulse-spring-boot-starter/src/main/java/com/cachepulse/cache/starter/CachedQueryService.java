package com.cachepulse.cache.starter;

import com.cachepulse.cache.core.CacheKeys;

import com.cachepulse.cache.starter.CacheLoadGuard.Lane;

import com.cachepulse.cache.core.CacheFreshness;
import com.cachepulse.cache.model.AutoCacheConfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 共享缓存查询：批量读取 Hash、只加载缺失 ID、复用同实例同 key 的在途加载。
 * 在途记录只保存尚未结束的加载，不构成本地数据缓存。
 */
@Service
public class CachedQueryService {
    public static final int NULL_CACHE_SECONDS = CacheKeys.NULL_CACHE_SECONDS;
    public static final int LOAD_TOKEN_SECONDS = 30;
    private static final String BEGIN_LOAD_SCRIPT = """
            local data = redis.call('HGET', KEYS[1], 'data')
            if data then return data end
            redis.call('HSET', KEYS[1], 'load_token', ARGV[1])
            redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
            return nil
            """;
    private static final String FILL_SCRIPT = """
            if redis.call('HGET', KEYS[1], 'load_token') ~= ARGV[3]
                    or redis.call('HEXISTS', KEYS[1], 'data') == 1 then
                return 0
            end
            redis.call('HSET', KEYS[1], 'data', ARGV[1], 'version', '0')
            redis.call('HDEL', KEYS[1], 'load_token')
            redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
            return 1
            """;
    private static final String CLEANUP_SCRIPT = """
            if redis.call('HGET', KEYS[1], 'load_token') ~= ARGV[1] then return 0 end
            return redis.call('HDEL', KEYS[1], 'load_token')
            """;
    private static final DefaultRedisScript<String> BEGIN_LOAD =
            new DefaultRedisScript<>(BEGIN_LOAD_SCRIPT, String.class);
    private static final DefaultRedisScript<Long> FILL_IF_TOKEN_MATCHES =
            new DefaultRedisScript<>(FILL_SCRIPT, Long.class);
    private static final DefaultRedisScript<Long> CLEANUP_LOAD =
            new DefaultRedisScript<>(CLEANUP_SCRIPT, Long.class);

    private static final TypeReference<List<Long>> ID_LIST = new TypeReference<>() { };
    /** 绕过缓存的加载单独合并，不与正常加载共用在途结果，避免拿到正常加载复查到的旧缓存。 */
    private static final String BYPASS_PREFIX = "bypass|";

    private final RedisTemplate<String, String> redis;
    private final CacheAnnotationProcessor processor;
    private final ObjectMapper objectMapper;
    private final MeterRegistry registry;
    private final CacheFreshness freshness;
    private final QueryProtection protection;
    private final ConcurrentHashMap<String, CompletableFuture<String>> inFlight = new ConcurrentHashMap<>();

    public CachedQueryService(RedisTemplate<String, String> redis, CacheAnnotationProcessor processor,
                              ObjectMapper objectMapper, MeterRegistry registry) {
        this(redis, processor, objectMapper, registry, CacheFreshness.ALWAYS);
    }

    public CachedQueryService(RedisTemplate<String, String> redis, CacheAnnotationProcessor processor,
                              ObjectMapper objectMapper, MeterRegistry registry, CacheFreshness freshness) {
        this(redis, processor, objectMapper, registry, freshness, QueryProtection.disabled());
    }

    @Autowired
    public CachedQueryService(RedisTemplate<String, String> redis, CacheAnnotationProcessor processor,
                              ObjectMapper objectMapper, MeterRegistry registry, CacheFreshness freshness,
                              QueryProtection protection) {
        this.redis = redis;
        this.processor = processor;
        this.objectMapper = objectMapper;
        this.registry = registry;
        this.freshness = freshness;
        this.protection = protection;
    }

    public <T> T getById(String table, Long id, Class<T> type, Supplier<T> loader) {
        BatchQueryResult<T> result = getByIds(table, List.of(id), type, ignored -> {
            T item = loader.get();
            return item == null ? List.of() : List.of(item);
        }, ignored -> id);
        return result.items().isEmpty() ? null : result.items().get(0);
    }

    public <T> BatchQueryResult<T> getByIds(String table, List<Long> ids, Class<T> type,
                                           Function<List<Long>, List<T>> loader,
                                           Function<T, Long> idExtractor) {
        List<Long> uniqueIds = ids.stream().distinct().toList();
        AutoCacheConfig config = processor.getConfig(table);
        // 心跳判定 CDC 链路中断或积压时，缓存可能是旧值：不读也不回填 Redis，直接查库。
        // Redis 出错或熔断时同样直接查库：数据库是准确来源，只是这段时间不经过缓存。
        boolean bypass = !freshness.cacheUsable();
        String bypassResult = "bypass";
        List<Object> cached = bypass ? null : readOrFallback(config, uniqueIds);
        if (!bypass && cached == null) {
            bypass = true;
            bypassResult = "redis_fallback";
        }
        if (bypass) {
            cached = Collections.nCopies(uniqueIds.size(), null);
        }
        Map<Long, String> values = new LinkedHashMap<>();
        Map<Long, CompletableFuture<String>> owned = new LinkedHashMap<>();
        Map<Long, CompletableFuture<String>> pending = new LinkedHashMap<>();

        for (int i = 0; i < uniqueIds.size(); i++) {
            Long id = uniqueIds.get(i);
            Object value = cached.get(i);
            registry.counter("cache.query.items", "table", table,
                    "result", bypass ? bypassResult : value == null ? "miss" : "hit").increment();
            if (value != null) {
                values.put(id, value.toString());
            } else {
                CompletableFuture<String> mine = new CompletableFuture<>();
                CompletableFuture<String> previous = inFlight.putIfAbsent(inFlightKey(config, id, bypass), mine);
                if (previous == null) {
                    owned.put(id, mine);
                    pending.put(id, mine);
                } else {
                    registry.counter("cache.query.coalesced", "table", table).increment();
                    pending.put(id, previous);
                }
            }
        }

        // 必须先完成自己负责的 ID，再等待其他加载者，避免重叠批次相互等待。
        if (!owned.isEmpty()) {
            loadOwned(table, config, owned, loader, idExtractor, bypass);
        }
        for (Map.Entry<Long, CompletableFuture<String>> entry : pending.entrySet()) {
            values.put(entry.getKey(), await(entry.getValue()));
        }

        List<T> items = new ArrayList<>();
        List<Long> missingIds = new ArrayList<>();
        for (Long id : uniqueIds) {
            T item = deserialize(values.get(id), type, config.buildCacheKeySimple(id));
            if (item == null) {
                missingIds.add(id);
            } else {
                items.add(item);
            }
        }
        return new BatchQueryResult<>(items, missingIds);
    }

    /**
     * 按二级索引查主键列表。索引缓存与主键缓存一样使用 load_token：失效 DEL 会删除标识，
     * 失效前开始的回源结果无法回填；空列表按空值缓存的短 TTL 保存。
     */
    public List<Long> getIndexedIds(String table, String column, String value, Supplier<List<Long>> loader) {
        AutoCacheConfig config = processor.getConfig(table);
        String key = config.indexKey(column).buildKey(value);
        boolean bypass = !freshness.cacheUsable();
        String bypassResult = "bypass";
        Object cached = null;
        if (!bypass) {
            bypass = !protection.redisAllowed();
            try {
                cached = bypass ? null : redis.opsForHash().get(key, "data");
                if (!bypass) {
                    protection.redisSucceeded();
                }
            } catch (DataAccessException e) {
                protection.redisFailed(e);
                bypass = true;
            }
            bypassResult = "redis_fallback";
        }
        registry.counter("cache.index.items", "table", table, "index", column,
                "result", bypass ? bypassResult : cached == null ? "miss" : "hit").increment();
        if (cached != null) {
            return parseIds(cached.toString(), key);
        }
        String flightKey = bypass ? BYPASS_PREFIX + key : key;
        CompletableFuture<String> mine = new CompletableFuture<>();
        CompletableFuture<String> previous = inFlight.putIfAbsent(flightKey, mine);
        if (previous != null) {
            registry.counter("cache.index.coalesced", "table", table, "index", column).increment();
            return parseIds(await(previous), key);
        }
        if (bypass) {
            try {
                String json = serialize(loadIndex(loader), key);
                mine.complete(json);
                return parseIds(json, key);
            } catch (RuntimeException | Error e) {
                mine.completeExceptionally(e);
                throw e;
            } finally {
                inFlight.remove(flightKey, mine);
            }
        }
        String token = UUID.randomUUID().toString();
        try {
            // 原子复查并登记本次加载，与主键缓存的回填保护相同。
            Object rechecked = null;
            try {
                rechecked = redis.execute(BEGIN_LOAD, List.of(key), token, String.valueOf(LOAD_TOKEN_SECONDS));
                protection.redisSucceeded();
            } catch (DataAccessException e) {
                // 登记失败：直接查库且不回填，也没有标识需要清理。
                protection.redisFailed(e);
                token = null;
            }
            String json;
            if (rechecked != null) {
                json = rechecked.toString();
            } else {
                json = serialize(loadIndex(loader), key);
                if (token != null) {
                    try {
                        Long outcome = redis.execute(FILL_IF_TOKEN_MATCHES, List.of(key), json,
                                String.valueOf(ttl(config, json)), token);
                        protection.redisSucceeded();
                        registry.counter("cache.index.fills", "table", table, "index", column,
                                "result", Long.valueOf(1).equals(outcome) ? "written" : "rejected").increment();
                    } catch (DataAccessException e) {
                        // 回填失败只是少缓存一次，查库结果照常返回；残留标识由 TTL 清理。
                        protection.redisFailed(e);
                        token = null;
                    }
                }
            }
            mine.complete(json);
            return parseIds(json, key);
        } catch (RuntimeException | Error e) {
            try {
                if (token != null) {
                    redis.execute(CLEANUP_LOAD, List.of(key), token);
                }
            } catch (RuntimeException | Error cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            mine.completeExceptionally(e);
            throw e;
        } finally {
            inFlight.remove(key, mine);
        }
    }

    private <T> void loadOwned(String table, AutoCacheConfig config,
                               Map<Long, CompletableFuture<String>> owned,
                               Function<List<Long>, List<T>> loader, Function<T, Long> idExtractor, boolean bypass) {
        Map<Long, String> tokens = new LinkedHashMap<>();
        Set<Long> unfinished = new LinkedHashSet<>(owned.keySet());
        boolean useRedis = !bypass;
        if (useRedis) {
            owned.keySet().forEach(id -> tokens.put(id, UUID.randomUUID().toString()));
        }
        try {
            List<Long> ids = new ArrayList<>(owned.keySet());
            // 原子复查并登记本次加载。失效 DEL 会同时清除 data 和 load_token。
            List<Object> rechecked = Collections.nCopies(ids.size(), null);
            if (useRedis) {
                try {
                    rechecked = beginLoad(config, tokens);
                    protection.redisSucceeded();
                } catch (DataAccessException e) {
                    // 登记失败：直接查库且不回填，也没有标识需要清理。
                    protection.redisFailed(e);
                    useRedis = false;
                }
            }
            List<Long> missing = new ArrayList<>();
            for (int i = 0; i < ids.size(); i++) {
                if (rechecked.get(i) == null) {
                    missing.add(ids.get(i));
                } else {
                    owned.get(ids.get(i)).complete(rechecked.get(i).toString());
                    unfinished.remove(ids.get(i));
                }
            }
            // 布隆过滤器判定一定不存在的主键单独一组走窄通道。正常组先查库并结束等待，
            // 窄通道被占满时只拒绝可疑组，合并在正常组上的其他请求不受影响。
            for (Map.Entry<Lane, List<Long>> group : protection.lanes(table, missing).entrySet()) {
                List<Long> groupIds = group.getValue();
                List<T> items = protection.load(group.getKey(), () -> {
                    registry.counter("cache.query.database.calls", "table", table).increment();
                    registry.counter("cache.query.database.ids", "table", table).increment(groupIds.size());
                    return loader.apply(groupIds);
                });
                Map<Long, T> loaded = new LinkedHashMap<>();
                for (T item : items) {
                    loaded.put(idExtractor.apply(item), item);
                }
                Map<Long, String> fills = new LinkedHashMap<>();
                for (Long id : groupIds) {
                    fills.put(id, serialize(loaded.get(id), config.buildCacheKeySimple(id)));
                }
                if (useRedis) {
                    try {
                        fill(table, config, fills, tokens);
                        protection.redisSucceeded();
                    } catch (DataAccessException e) {
                        // 回填失败只是少缓存一次，查库结果照常返回；残留标识由 TTL 清理。
                        protection.redisFailed(e);
                        useRedis = false;
                    }
                }
                fills.forEach((id, json) -> {
                    owned.get(id).complete(json);
                    unfinished.remove(id);
                });
            }
        } catch (RuntimeException | Error e) {
            try {
                if (useRedis) {
                    Map<Long, String> unfinishedTokens = new LinkedHashMap<>(tokens);
                    unfinishedTokens.keySet().retainAll(unfinished);
                    cleanup(config, unfinishedTokens);
                }
            } catch (RuntimeException | Error cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            // 等待者也要结束并收到失败；不能把数据库/Redis 故障缓存为不存在。
            unfinished.forEach(id -> owned.get(id).completeExceptionally(e));
            throw e;
        } finally {
            owned.forEach((id, future) -> inFlight.remove(inFlightKey(config, id, bypass), future));
        }
    }

    private static String inFlightKey(AutoCacheConfig config, Long id, boolean bypass) {
        String key = config.buildCacheKeySimple(id);
        return bypass ? BYPASS_PREFIX + key : key;
    }

    /** 返回 null 表示 Redis 熔断中或本次读取出错，调用方改为直接查库。 */
    private List<Object> readOrFallback(AutoCacheConfig config, List<Long> ids) {
        if (!protection.redisAllowed()) {
            return null;
        }
        try {
            List<Object> values = read(config, ids);
            protection.redisSucceeded();
            return values;
        } catch (DataAccessException e) {
            protection.redisFailed(e);
            return null;
        }
    }

    private List<Long> loadIndex(Supplier<List<Long>> loader) {
        return protection.load(Lane.NORMAL, () -> loader.get().stream().distinct().sorted().toList());
    }

    /** 等待其他请求的加载结果。查库被拒绝时在本线程重新抛出，让接口返回 503。 */
    private static String await(CompletableFuture<String> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof CacheLoadRejectedException rejected) {
                throw new CacheLoadRejectedException(rejected);
            }
            throw e;
        }
    }

    private List<Object> read(AutoCacheConfig config, List<Long> ids) {
        if (ids.size() == 1) {
            return Collections.singletonList(redis.opsForHash().get(config.buildCacheKeySimple(ids.get(0)), "data"));
        }
        return redis.executePipelined((RedisCallback<Object>) connection -> {
            for (Long id : ids) {
                connection.hashCommands().hGet(bytes(config.buildCacheKeySimple(id)), bytes("data"));
            }
            return null;
        }, redis.getStringSerializer());
    }

    private List<Object> beginLoad(AutoCacheConfig config, Map<Long, String> tokens) {
        if (tokens.size() == 1) {
            Long id = tokens.keySet().iterator().next();
            return Collections.singletonList(redis.execute(BEGIN_LOAD,
                    List.of(config.buildCacheKeySimple(id)), tokens.get(id), String.valueOf(LOAD_TOKEN_SECONDS)));
        }
        return redis.executePipelined((RedisCallback<Object>) connection -> {
            tokens.forEach((id, token) -> connection.scriptingCommands().eval(bytes(BEGIN_LOAD_SCRIPT),
                    ReturnType.VALUE, 1, bytes(config.buildCacheKeySimple(id)),
                    bytes(token), bytes(String.valueOf(LOAD_TOKEN_SECONDS))));
            return null;
        }, redis.getStringSerializer());
    }

    private void fill(String table, AutoCacheConfig config, Map<Long, String> values, Map<Long, String> tokens) {
        List<Object> outcomes;
        if (values.size() == 1) {
            Long id = values.keySet().iterator().next();
            String json = values.get(id);
            outcomes = Collections.singletonList(redis.execute(FILL_IF_TOKEN_MATCHES,
                    List.of(config.buildCacheKeySimple(id)), json, String.valueOf(ttl(config, json)), tokens.get(id)));
        } else {
            outcomes = redis.executePipelined((RedisCallback<Object>) connection -> {
                values.forEach((id, json) -> connection.scriptingCommands().eval(bytes(FILL_SCRIPT),
                        ReturnType.INTEGER, 1, bytes(config.buildCacheKeySimple(id)),
                        bytes(json), bytes(String.valueOf(ttl(config, json))), bytes(tokens.get(id))));
                return null;
            });
        }
        for (Object outcome : outcomes) {
            registry.counter("cache.query.fills", "table", table,
                    "result", Long.valueOf(1).equals(outcome) ? "written" : "rejected").increment();
        }
    }

    private void cleanup(AutoCacheConfig config, Map<Long, String> tokens) {
        if (tokens.size() == 1) {
            Long id = tokens.keySet().iterator().next();
            redis.execute(CLEANUP_LOAD, List.of(config.buildCacheKeySimple(id)), tokens.get(id));
        } else {
            redis.executePipelined((RedisCallback<Object>) connection -> {
                tokens.forEach((id, token) -> connection.scriptingCommands().eval(bytes(CLEANUP_SCRIPT),
                        ReturnType.INTEGER, 1, bytes(config.buildCacheKeySimple(id)), bytes(token)));
                return null;
            });
        }
    }

    private int ttl(AutoCacheConfig config, String json) {
        return config.jitteredSeconds("null".equals(json) || "[]".equals(json)
                ? NULL_CACHE_SECONDS : config.getExpireSeconds());
    }

    private List<Long> parseIds(String json, String key) {
        List<Long> ids;
        try {
            ids = objectMapper.readValue(json, ID_LIST);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Invalid cache data for " + key, e);
        }
        if (ids == null || ids.contains(null)) {
            throw new IllegalStateException("Invalid cache data for " + key);
        }
        return List.copyOf(ids);
    }

    private String serialize(Object value, String key) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize cache data for " + key, e);
        }
    }

    private <T> T deserialize(String json, Class<T> type, String key) {
        try {
            return objectMapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Invalid cache data for " + key, e);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
