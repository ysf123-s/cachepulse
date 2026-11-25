package com.cachepulse.sync;

import com.cachepulse.cache.core.CacheKeys;

import com.cachepulse.cache.core.CacheFreshness;
import com.cachepulse.common.model.BinlogEvent;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

/**
 * CDC 心跳：定时更新业务库心跳表的若干行。更新与业务变更走同一条 binlog → Canal → Kafka 链路，按主键散到各分区；
 * 心跳在所在批次确认后才记录，说明该分区在它之前的事件都已处理完。
 *
 * <p>写入成功但心跳迟迟不回来，说明链路中断或积压，缓存可能还是旧值，此时查询绕过缓存直接读库；
 * 心跳追上后，积压的失效也已执行，自动恢复使用缓存。写心跳本身失败多半是数据库故障，此时继续使用缓存。
 * 按单实例消费全部分区设计：expected-partitions 个分区都要收到心跳才算追上；分区被其他实例接管后，
 * 本实例记录的该分区心跳会过期并触发降级。多实例部署需把 expected-partitions 设为 0。</p>
 *
 * <p>心跳还用来发现消息丢失（而不只是延迟）：每次 UPDATE 的前镜像是该行上一次写入的值，与上次处理到的值不同，
 * 说明中间有心跳没有被处理，同一分区同一时段的业务变更也一样，例如消费位点被拨到最新、Canal 位点被重置。
 * 只告警（日志、指标、状态接口），不自动清缓存。</p>
 */
@Slf4j
@Component
public class CdcHeartbeatMonitor implements CacheFreshness {
    public enum State { DISABLED, STARTING, FRESH, STALE, WRITE_FAILING }

    /** OFF：心跳或 Redis 未启用；UNAVAILABLE：行镜像缺少心跳旧值（binlog_row_image=MINIMAL），无法比较。 */
    public enum LossDetection { OFF, ON, UNAVAILABLE }

    /**
     * 一次检测到的丢失：这些分区在 fromMs 到 toMs 之间写入的心跳有一部分没有被处理，
     * 同一时段落在这些分区的业务变更可能也没有删除缓存。heartbeatRows 是跳号的心跳行数。
     */
    public record Loss(List<Integer> partitions, int heartbeatRows, long fromMs, long toMs, long detectedAtMs) {
    }

    /**
     * partitionAgeMs：各分区最近处理的心跳距今多久，即链路延迟加写入间隔。
     * uncoveredPartitions：没有心跳行散到的分区，这些分区的积压发现不了，需要增加 rows。
     * lossEvents / lastLoss：本进程检测到的消息丢失次数和最近一次的范围。
     */
    public record Status(State state, boolean cacheBypassed, long staleAfterMs, Long lastWriteAgeMs,
                         String lastWriteError, Map<Integer, Long> partitionAgeMs, List<Integer> uncoveredPartitions,
                         LossDetection lossDetection, long lossEvents, Loss lastLoss) {
    }

    /** 心跳链基线：心跳行 id → 最近处理到的 beat_ms。属于组件保留的 cachepulse: 命名空间。 */
    static final String CHAIN_KEY = CacheKeys.HEARTBEAT_CHAIN;
    /**
     * ARGV 每三个一组：行 id、前镜像 beat_ms（INSERT 或缺失时为空）、后镜像 beat_ms（DELETE 时为空）。
     * 前镜像等于基线、没有基线或没有前镜像时直接推进；前镜像不等于基线而后镜像更新时记为跳号；
     * 后镜像不比基线新的是重复投递，忽略。返回跳号行的 id、基线、后镜像。
     */
    private static final DefaultRedisScript<List> CHAIN_SCRIPT = new DefaultRedisScript<>("""
            local gaps = {}
            for i = 1, #ARGV, 3 do
                local row, before, after = ARGV[i], ARGV[i + 1], ARGV[i + 2]
                if after == '' then
                    redis.call('HDEL', KEYS[1], row)
                else
                    local last = redis.call('HGET', KEYS[1], row)
                    if (not last) or before == '' or before == last then
                        redis.call('HSET', KEYS[1], row, after)
                    elseif tonumber(after) > tonumber(last) then
                        gaps[#gaps + 1] = row
                        gaps[#gaps + 1] = last
                        gaps[#gaps + 1] = after
                        redis.call('HSET', KEYS[1], row, after)
                    end
                end
            end
            return gaps
            """, List.class);

    private static final Pattern TABLE_NAME = Pattern.compile("[A-Za-z0-9_]+");
    private static final int UNKNOWN_PARTITION = -1;

    private final boolean enabled;
    private final String table;
    private final int rows;
    /** 本实例写入的第一行心跳。原项目用 1～24，CachePulse 用 1001～1024。 */
    @Value("${canal.heartbeat.first-id:1}")
    private long firstId = 1;
    /** 心跳表所在的业务库；为空时不检查库名（单元测试）。 */
    @Value("${canal.heartbeat.database:#{null}}")
    private String database;
    private final long intervalMs;
    private final long staleAfterMs;
    private final int expectedPartitions;
    private final JdbcTemplate jdbc;
    private final LongSupplier clock;
    private final Map<Integer, Long> lastBeatByPartition = new ConcurrentHashMap<>();
    private final RedisTemplate<String, String> redis;
    private final String chainKey;
    private final Counter lossCounter;
    private final AtomicLong lossEvents = new AtomicLong();
    private volatile boolean chainUnavailable;
    private volatile Loss lastLoss;
    private volatile long firstWriteAt = -1;
    private volatile long lastWriteAt = -1;
    private volatile String lastWriteError;
    private volatile State loggedState;
    private ScheduledExecutorService scheduler;

    @Autowired
    public CdcHeartbeatMonitor(@Value("${canal.heartbeat.enabled:false}") boolean enabled,
                               @Value("${canal.heartbeat.table:cdc_heartbeat}") String table,
                               @Value("${canal.heartbeat.rows:24}") int rows,
                               @Value("${canal.heartbeat.interval-ms:1000}") long intervalMs,
                               @Value("${canal.heartbeat.stale-after-ms:3000}") long staleAfterMs,
                               @Value("${canal.heartbeat.expected-partitions:0}") int expectedPartitions,
                               DataSource dataSource, RedisTemplate<String, String> redis, MeterRegistry registry) {
        this(enabled, table, rows, intervalMs, staleAfterMs, expectedPartitions, timedJdbc(dataSource, staleAfterMs),
                System::currentTimeMillis, registry, redis, CHAIN_KEY);
    }

    CdcHeartbeatMonitor(boolean enabled, String table, int rows, long intervalMs, long staleAfterMs,
                        int expectedPartitions, JdbcTemplate jdbc, LongSupplier clock, MeterRegistry registry) {
        this(enabled, table, rows, intervalMs, staleAfterMs, expectedPartitions, jdbc, clock, registry, null, CHAIN_KEY);
    }

    CdcHeartbeatMonitor(boolean enabled, String table, int rows, long intervalMs, long staleAfterMs,
                        int expectedPartitions, JdbcTemplate jdbc, LongSupplier clock, MeterRegistry registry,
                        RedisTemplate<String, String> redis, String chainKey) {
        if (!TABLE_NAME.matcher(table).matches()) {
            throw new IllegalArgumentException("Invalid heartbeat table name: " + table);
        }
        if (rows < 1 || intervalMs < 1 || staleAfterMs <= intervalMs) {
            throw new IllegalArgumentException("Heartbeat needs rows >= 1 and stale-after-ms > interval-ms");
        }
        this.enabled = enabled;
        this.table = table;
        this.rows = rows;
        this.intervalMs = intervalMs;
        this.staleAfterMs = staleAfterMs;
        this.expectedPartitions = expectedPartitions;
        this.jdbc = jdbc;
        this.clock = clock;
        this.redis = redis;
        this.chainKey = chainKey;
        if (registry != null) {
            Gauge.builder("canal.heartbeat.age.ms", this, CdcHeartbeatMonitor::maxAgeMs)
                    .description("Age of the oldest per-partition heartbeat processed; -1 before any heartbeat")
                    .register(registry);
            lossCounter = Counter.builder("canal.heartbeat.loss.detected")
                    .description("Times skipped heartbeats showed that CDC changes were lost, not just delayed")
                    .register(registry);
        } else {
            lossCounter = null;
        }
    }

    public static CdcHeartbeatMonitor disabled() {
        return new CdcHeartbeatMonitor(false, "cdc_heartbeat", 1, 1_000, 3_000, 0, null, System::currentTimeMillis, null);
    }

    private static JdbcTemplate timedJdbc(DataSource dataSource, long staleAfterMs) {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        // 数据库卡住时写入也要按时结束，否则会一直停在 STARTING/FRESH。
        jdbc.setQueryTimeout((int) Math.max(1, staleAfterMs / 1_000));
        return jdbc;
    }

    @PostConstruct
    public void start() {
        if (!enabled) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "cdc-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.scheduleWithFixedDelay(this::beat, 0, intervalMs, TimeUnit.MILLISECONDS);
        log.info("CDC heartbeat started: table={}, rows={}, intervalMs={}, staleAfterMs={}",
                table, rows, intervalMs, staleAfterMs);
    }

    @PreDestroy
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    void beat() {
        long now = clock.getAsLong();
        try {
            if (jdbc.update("UPDATE " + table + " SET beat_ms = ? WHERE id BETWEEN ? AND ?", now, firstId, firstId + rows - 1) < rows) {
                Object[] args = new Object[rows * 2];
                for (int i = 0; i < rows; i++) {
                    args[i * 2] = firstId + i;
                    args[i * 2 + 1] = now;
                }
                jdbc.update("INSERT IGNORE INTO " + table + " (id, beat_ms) VALUES "
                        + String.join(",", Collections.nCopies(rows, "(?,?)")), args);
            }
            if (firstWriteAt < 0) {
                firstWriteAt = now;
            }
            lastWriteAt = now;
            lastWriteError = null;
        } catch (RuntimeException e) {
            lastWriteError = e.getClass().getSimpleName() + ": " + e.getMessage();
        }
        logTransition();
    }

    /**
     * 只认本实例负责的心跳行。开发期间原项目也在写同一张表（1～24 行），别人的行当作普通变更忽略：
     * 两个写入方写同一行时，时钟偏差会让基线比较误报丢消息。
     */
    public boolean isHeartbeat(BinlogEvent event) {
        if (!enabled || event.isDdl() || !table.equals(event.getTableName())
                || (database != null && !database.equals(event.getDatabaseName()))) {
            return false;
        }
        Map<String, Object> image = event.getAfterData() != null ? event.getAfterData() : event.getBeforeData();
        Object id = image == null ? null : image.get("id");
        try {
            long row = Long.parseLong(String.valueOf(id));
            return row >= firstId && row < firstId + rows;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * 检查心跳链是否连续，在心跳所在批次确认之前调用。基线存在 Redis，随批次一起推进：Redis 出错时整批回滚重试，
     * 重启后也能接着比较；Redis 被清空时缓存也一起清空，不会有旧值残留，基线从下一次心跳重新开始。
     * 重复投递的心跳不比基线新，忽略，不会误报。
     */
    public void checkContinuity(List<BinlogEvent> heartbeats) {
        if (!enabled || redis == null || heartbeats.isEmpty()) {
            return;
        }
        List<String> args = new ArrayList<>();
        Map<String, Integer> partitionByRow = new HashMap<>();
        for (BinlogEvent event : heartbeats) {
            boolean deleted = event.getEventType() == EventType.DELETE;
            Map<String, Object> image = deleted ? event.getBeforeData() : event.getAfterData();
            Object row = image == null ? null : image.get("id");
            Long after = deleted ? null : beatMs(event.getAfterData());
            if (row == null || (!deleted && after == null)) {
                continue;
            }
            Long before = event.getEventType() == EventType.UPDATE ? beatMs(event.getBeforeData()) : null;
            if (event.getEventType() == EventType.UPDATE && before == null) {
                chainUnavailable = true;
            }
            args.add(row.toString());
            args.add(before == null ? "" : before.toString());
            args.add(after == null ? "" : after.toString());
            Integer partition = event.getKafkaPartition();
            partitionByRow.put(row.toString(), partition == null ? UNKNOWN_PARTITION : partition);
        }
        if (args.isEmpty()) {
            return;
        }
        List<?> gaps = redis.execute(CHAIN_SCRIPT, List.of(chainKey), args.toArray());
        if (gaps != null && !gaps.isEmpty()) {
            reportLoss(gaps, partitionByRow);
        }
    }

    private void reportLoss(List<?> gaps, Map<String, Integer> partitionByRow) {
        Set<Integer> partitions = new TreeSet<>();
        long from = Long.MAX_VALUE;
        long to = Long.MIN_VALUE;
        int rowCount = 0;
        for (int i = 0; i + 2 < gaps.size(); i += 3) {
            partitions.add(partitionByRow.getOrDefault(String.valueOf(gaps.get(i)), UNKNOWN_PARTITION));
            from = Math.min(from, Long.parseLong(String.valueOf(gaps.get(i + 1))));
            to = Math.max(to, Long.parseLong(String.valueOf(gaps.get(i + 2))));
            rowCount++;
        }
        if (rowCount == 0) {
            return;
        }
        lastLoss = new Loss(List.copyOf(partitions), rowCount, from, to, clock.getAsLong());
        lossEvents.incrementAndGet();
        if (lossCounter != null) {
            lossCounter.increment();
        }
        log.error("CDC changes lost on partitions {}: {} heartbeat rows skipped updates written between {} and {} "
                        + "(epoch ms). Row changes committed in that window were never processed, so their cache entries "
                        + "may stay stale until TTL. Look them up with GET /platform/events?from={}&to={} "
                        + "or clear a group with POST /cache/groups/<table>/purge",
                partitions, rowCount, from, to, from, to);
    }

    /** 只能在心跳所在批次确认之后调用：确认前记录会把卡在重试中的分区误判为正常。 */
    public void recordProcessed(List<BinlogEvent> heartbeats) {
        for (BinlogEvent event : heartbeats) {
            Long beat = beatMs(event);
            if (beat != null) {
                Integer partition = event.getKafkaPartition();
                lastBeatByPartition.merge(partition == null ? UNKNOWN_PARTITION : partition, beat, Math::max);
            }
        }
    }

    public State state() {
        if (!enabled) {
            return State.DISABLED;
        }
        long now = clock.getAsLong();
        if (lastWriteAt < 0) {
            return lastWriteError == null ? State.STARTING : State.WRITE_FAILING;
        }
        if (now - lastWriteAt > staleAfterMs) {
            return State.WRITE_FAILING;
        }
        // 每个分区都要用心跳证明自己追上了；分区数未知时只看收到过心跳的分区。
        if (lastBeatByPartition.isEmpty() || !uncoveredPartitions().isEmpty()) {
            return now - firstWriteAt > staleAfterMs ? State.STALE : State.STARTING;
        }
        for (long beat : lastBeatByPartition.values()) {
            if (now - beat > staleAfterMs) {
                return State.STALE;
            }
        }
        return State.FRESH;
    }

    /** 启动后尚未确认链路、或链路中断/积压时不用缓存；写心跳失败（数据库故障）时继续用缓存。 */
    @Override
    public boolean cacheUsable() {
        State state = state();
        return state != State.STALE && state != State.STARTING;
    }

    public Status status() {
        long now = clock.getAsLong();
        Map<Integer, Long> ages = new TreeMap<>();
        lastBeatByPartition.forEach((partition, beat) -> ages.put(partition, Math.max(0, now - beat)));
        return new Status(state(), !cacheUsable(), staleAfterMs,
                lastWriteAt < 0 ? null : Math.max(0, now - lastWriteAt), lastWriteError, ages, uncoveredPartitions(),
                lossDetection(), lossEvents.get(), lastLoss);
    }

    private LossDetection lossDetection() {
        if (!enabled || redis == null) {
            return LossDetection.OFF;
        }
        return chainUnavailable ? LossDetection.UNAVAILABLE : LossDetection.ON;
    }

    /** FlatMessage 模式下事件不带分区号，无法判断覆盖情况。 */
    private List<Integer> uncoveredPartitions() {
        if (lastBeatByPartition.containsKey(UNKNOWN_PARTITION)) {
            return List.of();
        }
        return IntStream.range(0, expectedPartitions).filter(p -> !lastBeatByPartition.containsKey(p)).boxed().toList();
    }

    private double maxAgeMs() {
        long now = clock.getAsLong();
        return lastBeatByPartition.values().stream().mapToLong(beat -> now - beat).max().orElse(-1);
    }

    private void logTransition() {
        State state = state();
        if (state == loggedState) {
            return;
        }
        loggedState = state;
        if (state == State.STALE) {
            // 长期没有心跳的分区，可能是心跳行没有散到它，需要增加 canal.heartbeat.rows。
            log.warn("CDC heartbeat stale (> {} ms): queries bypass Redis and read MySQL until heartbeats catch up; "
                    + "ages={}, partitions without heartbeat={}", staleAfterMs, status().partitionAgeMs(), uncoveredPartitions());
        } else if (state == State.WRITE_FAILING) {
            log.warn("CDC heartbeat writes failing, cache stays in use: {}", lastWriteError);
        } else {
            log.info("CDC heartbeat {}: ages={}", state, status().partitionAgeMs());
        }
    }

    private static Long beatMs(BinlogEvent event) {
        return beatMs(event.getAfterData());
    }

    private static Long beatMs(Map<String, Object> row) {
        Object value = row == null ? null : row.get("beat_ms");
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
