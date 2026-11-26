package com.cachepulse.sync;

import com.cachepulse.common.model.BinlogEvent;
import com.cachepulse.sync.CdcHeartbeatMonitor.State;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CdcHeartbeatMonitorTest {
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final RedisTemplate<String, String> redis = mock(RedisTemplate.class);
    private final AtomicLong now = new AtomicLong(1_000_000);
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final CdcHeartbeatMonitor monitor =
            new CdcHeartbeatMonitor(true, "cdc_heartbeat", 3, 1_000, 3_000, 2, jdbc, now::get, metrics);

    @Test
    void disabledMonitorKeepsCacheAndIgnoresEvents() {
        CdcHeartbeatMonitor disabled = CdcHeartbeatMonitor.disabled();

        assertThat(disabled.state()).isEqualTo(State.DISABLED);
        assertThat(disabled.cacheUsable()).isTrue();
        assertThat(disabled.isHeartbeat(heartbeat(0, 1))).isFalse();
    }

    @Test
    void bypassesCacheUntilTheFirstHeartbeatComesBack() {
        assertThat(monitor.state()).isEqualTo(State.STARTING);
        assertThat(monitor.cacheUsable()).isFalse();

        when(jdbc.update(startsWith("UPDATE cdc_heartbeat"), any(Object[].class))).thenReturn(3);
        monitor.beat();
        long written = now.get();
        now.addAndGet(200);
        monitor.recordProcessed(List.of(heartbeat(0, written), heartbeat(1, written)));

        assertThat(monitor.state()).isEqualTo(State.FRESH);
        assertThat(monitor.cacheUsable()).isTrue();
        assertThat(monitor.status().partitionAgeMs()).isEqualTo(Map.of(0, 200L, 1, 200L));
        assertThat(metrics.get("canal.heartbeat.age.ms").gauge().value()).isEqualTo(200);
    }

    @Test
    void anyPartitionFallingBehindBypassesCacheUntilItCatchesUp() {
        when(jdbc.update(startsWith("UPDATE cdc_heartbeat"), any(Object[].class))).thenReturn(3);
        monitor.beat();
        monitor.recordProcessed(List.of(heartbeat(0, now.get()), heartbeat(1, now.get())));

        now.addAndGet(3_500);
        monitor.beat();
        monitor.recordProcessed(List.of(heartbeat(0, now.get())));

        assertThat(monitor.state()).isEqualTo(State.STALE);
        assertThat(monitor.cacheUsable()).isFalse();
        assertThat(monitor.status().cacheBypassed()).isTrue();

        monitor.recordProcessed(List.of(heartbeat(1, now.get())));
        assertThat(monitor.state()).isEqualTo(State.FRESH);
    }

    @Test
    void heartbeatsThatNeverArriveAfterSuccessfulWritesMeanThePipelineIsDown() {
        when(jdbc.update(startsWith("UPDATE cdc_heartbeat"), any(Object[].class))).thenReturn(3);
        monitor.beat();
        now.addAndGet(2_000);
        monitor.beat();
        assertThat(monitor.state()).isEqualTo(State.STARTING);

        now.addAndGet(1_500);
        monitor.beat();
        assertThat(monitor.state()).isEqualTo(State.STALE);
    }

    @Test
    void failingOrStalledWritesBypassCacheUntilHeartbeatsRecover() {
        when(jdbc.update(startsWith("UPDATE cdc_heartbeat"), any(Object[].class)))
                .thenThrow(new DataAccessResourceFailureException("MySQL unavailable"));
        monitor.beat();
        assertThat(monitor.state()).isEqualTo(State.WRITE_FAILING);
        assertThat(monitor.cacheUsable()).isFalse();
        assertThat(monitor.status().cacheBypassed()).isTrue();
        assertThat(monitor.status().lastWriteError()).contains("MySQL unavailable");

        reset(jdbc);
        when(jdbc.update(startsWith("UPDATE cdc_heartbeat"), any(Object[].class))).thenReturn(3);
        monitor.beat();
        monitor.recordProcessed(List.of(heartbeat(0, now.get()), heartbeat(1, now.get())));
        assertThat(monitor.state()).isEqualTo(State.FRESH);

        now.addAndGet(3_500);
        assertThat(monitor.state()).isEqualTo(State.WRITE_FAILING);
        assertThat(monitor.cacheUsable()).isFalse();

        monitor.beat();
        monitor.recordProcessed(List.of(heartbeat(0, now.get())));
        assertThat(monitor.state()).isEqualTo(State.STALE);
        assertThat(monitor.cacheUsable()).isFalse();

        monitor.recordProcessed(List.of(heartbeat(1, now.get())));
        assertThat(monitor.state()).isEqualTo(State.FRESH);
        assertThat(monitor.cacheUsable()).isTrue();
    }

    @Test
    void partitionThatNeverReceivesAHeartbeatKeepsBypassingAndIsReported() {
        when(jdbc.update(startsWith("UPDATE cdc_heartbeat"), any(Object[].class))).thenReturn(3);
        monitor.beat();
        monitor.recordProcessed(List.of(heartbeat(0, now.get())));
        assertThat(monitor.state()).isEqualTo(State.STARTING);

        now.addAndGet(3_500);
        monitor.beat();
        monitor.recordProcessed(List.of(heartbeat(0, now.get())));

        assertThat(monitor.state()).isEqualTo(State.STALE);
        assertThat(monitor.status().uncoveredPartitions()).containsExactly(1);
    }

    @Test
    void missingRowsAreSeededInOneStatement() {
        when(jdbc.update(startsWith("UPDATE cdc_heartbeat"), any(Object[].class))).thenReturn(1);

        monitor.beat();

        long t = now.get();
        verify(jdbc).update("INSERT IGNORE INTO cdc_heartbeat (id, beat_ms) VALUES (?,?),(?,?),(?,?)",
                1L, t, 2L, t, 3L, t);
    }

    @Test
    void recognisesOnlyRowChangesOfTheHeartbeatTable() {
        BinlogEvent ddl = heartbeat(0, 1);
        ddl.setDdl(true);
        BinlogEvent other = heartbeat(0, 1);
        other.setTableName("users");

        assertThat(monitor.isHeartbeat(heartbeat(0, 1))).isTrue();
        assertThat(monitor.isHeartbeat(ddl)).isFalse();
        assertThat(monitor.isHeartbeat(other)).isFalse();
    }

    /** 设计文档第 6.5 节：原项目写 1～24 行，CachePulse 只认自己写的 1001～1024 行，而且只认配置的业务库。 */
    @Test
    void recognisesOnlyItsOwnRowsOfTheConfiguredDatabase() {
        CdcHeartbeatMonitor own = new CdcHeartbeatMonitor(true, "cdc_heartbeat", 24, 1_000, 3_000, 0, jdbc, now::get, null);
        ReflectionTestUtils.setField(own, "firstId", 1001L);
        ReflectionTestUtils.setField(own, "database", "demo_db");

        for (String row : List.of("1001", "1012", "1024")) {
            assertThat(own.isHeartbeat(row("demo_db", "cdc_heartbeat", row))).as(row).isTrue();
        }
        for (String row : List.of("1", "24", "1000", "1025", "x")) {
            assertThat(own.isHeartbeat(row("demo_db", "cdc_heartbeat", row))).as(row).isFalse();
        }
        assertThat(own.isHeartbeat(row("other_db", "cdc_heartbeat", "1001"))).isFalse();
        assertThat(own.isHeartbeat(row("demo_db", "users", "1001"))).isFalse();
        BinlogEvent deleted = row("demo_db", "cdc_heartbeat", "1001");
        deleted.setBeforeData(deleted.getAfterData());
        deleted.setAfterData(null);
        assertThat(own.isHeartbeat(deleted)).isTrue();
    }

    @Test
    void writesAndSeedsOnlyItsOwnRows() {
        CdcHeartbeatMonitor own = new CdcHeartbeatMonitor(true, "cdc_heartbeat", 3, 1_000, 3_000, 0, jdbc, now::get, null);
        ReflectionTestUtils.setField(own, "firstId", 1001L);
        when(jdbc.update(startsWith("UPDATE cdc_heartbeat"), any(Object[].class))).thenReturn(0);

        own.beat();

        long t = now.get();
        verify(jdbc).update("UPDATE cdc_heartbeat SET beat_ms = ? WHERE id BETWEEN ? AND ?", t, 1001L, 1003L);
        verify(jdbc).update("INSERT IGNORE INTO cdc_heartbeat (id, beat_ms) VALUES (?,?),(?,?),(?,?)",
                1001L, t, 1002L, t, 1003L, t);
    }

    private static BinlogEvent row(String database, String table, String id) {
        BinlogEvent event = heartbeat(0, 1);
        event.setDatabaseName(database);
        event.setTableName(table);
        event.setAfterData(Map.of("id", id, "beat_ms", "1"));
        return event;
    }

    @Test
    void rejectsUnsafeTableNamesAndThresholds() {
        assertThatThrownBy(() -> new CdcHeartbeatMonitor(true, "t; DROP TABLE users", 1, 1_000, 3_000, 0, jdbc, now::get, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CdcHeartbeatMonitor(true, "cdc_heartbeat", 1, 1_000, 1_000, 0, jdbc, now::get, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void continuousHeartbeatChainReportsNoLoss() {
        CdcHeartbeatMonitor chained = chained(new SimpleMeterRegistry());
        List<List<Object>> calls = recordChainCalls(List.of());

        chained.checkContinuity(List.of(update(0, "1", 100, 200), update(1, "2", 100, 200)));

        assertThat(calls).containsExactly(List.of("1", "100", "200", "2", "100", "200"));
        assertThat(chained.status().lossDetection()).isEqualTo(CdcHeartbeatMonitor.LossDetection.ON);
        assertThat(chained.status().lossEvents()).isZero();
        assertThat(chained.status().lastLoss()).isNull();
    }

    @Test
    void skippedHeartbeatsAreReportedWithTheAffectedPartitionsAndWindow() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        CdcHeartbeatMonitor chained = chained(registry);
        recordChainCalls(List.of("1", "200", "5000", "2", "300", "5000"));

        chained.checkContinuity(List.of(update(3, "1", 4000, 5000), update(5, "2", 4000, 5000)));

        CdcHeartbeatMonitor.Loss loss = chained.status().lastLoss();
        assertThat(chained.status().lossEvents()).isEqualTo(1);
        assertThat(loss.partitions()).containsExactly(3, 5);
        assertThat(loss.heartbeatRows()).isEqualTo(2);
        assertThat(loss.fromMs()).isEqualTo(200);
        assertThat(loss.toMs()).isEqualTo(5000);
        assertThat(loss.detectedAtMs()).isEqualTo(now.get());
        assertThat(registry.get("canal.heartbeat.loss.detected").counter().count()).isEqualTo(1);
    }

    @Test
    void insertsDeletesAndMinimalRowImagesPassEmptyImages() {
        CdcHeartbeatMonitor chained = chained(new SimpleMeterRegistry());
        List<List<Object>> calls = recordChainCalls(List.of());
        BinlogEvent insert = heartbeat(0, 100);
        insert.setEventType(EventType.INSERT);
        BinlogEvent delete = heartbeat(0, 100);
        delete.setEventType(EventType.DELETE);
        delete.setBeforeData(Map.of("id", "1", "beat_ms", "100"));
        delete.setAfterData(null);
        BinlogEvent minimal = heartbeat(0, 200);
        minimal.setBeforeData(Map.of("id", "1"));

        chained.checkContinuity(List.of(insert, delete, minimal));

        assertThat(calls).containsExactly(List.of("1", "", "100", "1", "", "", "1", "", "200"));
        assertThat(chained.status().lossDetection()).isEqualTo(CdcHeartbeatMonitor.LossDetection.UNAVAILABLE);
    }

    @Test
    void lossDetectionIsOffWithoutRedis() {
        monitor.checkContinuity(List.of(update(0, "1", 100, 200)));

        assertThat(monitor.status().lossDetection()).isEqualTo(CdcHeartbeatMonitor.LossDetection.OFF);
        verifyNoInteractions(redis);
    }

    private CdcHeartbeatMonitor chained(SimpleMeterRegistry registry) {
        return new CdcHeartbeatMonitor(true, "cdc_heartbeat", 3, 1_000, 3_000, 2, jdbc, now::get, registry,
                redis, "test-chain");
    }

    private List<List<Object>> recordChainCalls(List<String> gaps) {
        List<List<Object>> calls = new ArrayList<>();
        when(redis.execute(any(RedisScript.class), eq(List.of("test-chain")), any(Object[].class)))
                .thenAnswer(invocation -> {
                    Object[] args = invocation.getArguments();
                    calls.add(List.of(Arrays.copyOfRange(args, 2, args.length)));
                    return gaps;
                });
        return calls;
    }

    private static BinlogEvent update(int partition, String row, long before, long after) {
        BinlogEvent event = heartbeat(partition, after);
        event.setAfterData(Map.of("id", row, "beat_ms", String.valueOf(after)));
        event.setBeforeData(Map.of("id", row, "beat_ms", String.valueOf(before)));
        return event;
    }

    private static BinlogEvent heartbeat(int partition, long beatMs) {
        BinlogEvent event = new BinlogEvent();
        event.setTableName("cdc_heartbeat");
        event.setEventType(EventType.UPDATE);
        event.setKafkaPartition(partition);
        event.setAfterData(Map.<String, Object>of("id", "1", "beat_ms", String.valueOf(beatMs)));
        return event;
    }
}
