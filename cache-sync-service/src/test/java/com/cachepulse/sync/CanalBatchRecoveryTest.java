package com.cachepulse.sync;

import com.cachepulse.cache.annotation.CacheStrategy;
import com.cachepulse.cache.core.CacheGroups;
import com.cachepulse.cache.core.CacheKeys;
import com.cachepulse.cache.model.AutoCacheConfig;
import com.cachepulse.common.kafka.CanalConnectorWrapper;
import com.cachepulse.common.model.BinlogVersion;
import com.cachepulse.common.model.KafkaCanalRecord;
import com.cachepulse.common.parser.BinlogParser;
import com.cachepulse.common.parser.FlatMessageBinlogParser;
import com.cachepulse.sync.diagnostics.TraceCollector;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CanalBatchRecoveryTest {
    private final CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
    private final CanalConnectorWrapper.PartitionConsumer consumer = mock(CanalConnectorWrapper.PartitionConsumer.class);
    private final CacheGroups processor = mock(CacheGroups.class, CALLS_REAL_METHODS);
    private final RedisTemplate<String, String> redis = mock(RedisTemplate.class);
    private final VersionControllerWithLua versions = new VersionControllerWithLua(redis);
    private final AutoCacheConfig config = new AutoCacheConfig();
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private CanalCacheSyncService service;

    @BeforeEach
    void setUp() {
        config.setDatabase("demo_db");
        config.setTable("users");
        config.setIdField("id");
        config.setKeyTemplate("user:#id");
        config.setStrategy(CacheStrategy.DELETE_MODE);
        config.setEnableVersionControl(true);
        when(processor.getConfig("users")).thenReturn(config);
        service = new CanalCacheSyncService(connector, new BinlogParser(),
                new FlatMessageBinlogParser(), processor, versions,
                mock(BigKeyDetector.class), redis, new ObjectMapper(), metrics);
    }

    @Test
    @Timeout(10)
    void redisFailureRollsBackAndRetriesBeforeAcknowledging() {
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(record(CanalEntry.EventType.UPDATE)));
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenThrow(new RedisConnectionFailureException("Redis unavailable"))
                .thenReturn(1L);
        ReflectionTestUtils.setField(service, "running", true);
        doAnswer(invocation -> {
            ReflectionTestUtils.setField(service, "running", false);
            return null;
        }).when(consumer).ack();

        ReflectionTestUtils.invokeMethod(service, "doSyncLoop", consumer);

        var order = inOrder(consumer);
        order.verify(consumer).getRecordsWithoutAck();
        order.verify(consumer).rollback();
        order.verify(consumer).getRecordsWithoutAck();
        order.verify(consumer).ack();
        verify(consumer, times(1)).ack();
        assertThat(service.getStatus().lastSuccessAt()).isNotNull();
        assertThat(service.getStatus().lastError().type()).isEqualTo("RedisConnectionFailureException");
        assertThat(service.getStatus().lastSuccessAt())
                .isAfterOrEqualTo(service.getStatus().lastError().occurredAt());
    }

    @Test
    void everyRedisWriteStrategyPropagatesConnectionFailure() {
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenThrow(new RedisConnectionFailureException("Redis unavailable"));
        BinlogVersion version = new BinlogVersion(0, 1, 100, (1L << 32) | 100);

        assertThatThrownBy(() -> versions.deleteIfNewerVersion("user:1", version))
                .isInstanceOf(RedisConnectionFailureException.class);
        assertThatThrownBy(() -> versions.updateIfNewerVersion("user:1", "{}", version, 60))
                .isInstanceOf(RedisConnectionFailureException.class);
        assertThatThrownBy(() -> versions.updateIfPresent("user:1", "{}", 60))
                .isInstanceOf(RedisConnectionFailureException.class);
    }

    @Test
    void malformedEntryIsDeadLetteredWhileOtherEntriesAreProcessed() {
        config.setEnableVersionControl(false);
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(
                recordOf(malformedEntry(), rowEntry(CanalEntry.EventType.INSERT, column("id", "1")))));
        when(redis.delete("user:1")).thenReturn(true);

        assertThat(service.processBatch(consumer)).isTrue();

        var order = inOrder(redis, consumer);
        order.verify(redis).delete("user:1");
        order.verify(redis).execute(any(RedisScript.class), eq(List.of(DeadLetterStore.STREAM_KEY)), any(Object[].class));
        order.verify(consumer).ack();
        assertThat(deadLettered("MALFORMED_ENTRY")).isEqualTo(1);
    }

    @Test
    void missingPrimaryKeyIsDeadLetteredAndLaterRowsStillInvalidate() {
        config.setEnableVersionControl(false);
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(recordOf(
                rowEntry(CanalEntry.EventType.UPDATE, column("name", "no id")),
                rowEntry(CanalEntry.EventType.UPDATE, column("id", "2")))));
        when(redis.delete("user:2")).thenReturn(true);

        assertThat(service.processBatch(consumer)).isTrue();

        verify(redis).delete("user:2");
        verify(consumer).ack();
        assertThat(deadLettered("MISSING_PRIMARY_KEY")).isEqualTo(1);
    }

    @Test
    void deadLetterWriteFailureRollsBackInsteadOfLosingTheEvent() {
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(recordOf(malformedEntry())));
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenThrow(new RedisConnectionFailureException("Redis unavailable"));

        assertThatThrownBy(() -> service.processBatch(consumer)).isInstanceOf(RedisConnectionFailureException.class);

        verify(consumer, never()).ack();
        assertThat(deadLettered("MALFORMED_ENTRY")).isZero();
    }

    @Test
    void failedBatchWritesNoDeadLettersSoRetriesCannotDuplicateThem() {
        config.setEnableVersionControl(false);
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(
                recordOf(malformedEntry(), rowEntry(CanalEntry.EventType.INSERT, column("id", "1")))));
        when(redis.delete("user:1")).thenThrow(new RedisConnectionFailureException("Redis unavailable"));

        assertThatThrownBy(() -> service.processBatch(consumer)).isInstanceOf(RedisConnectionFailureException.class);

        verify(redis, never()).execute(any(RedisScript.class), anyList(), any(Object[].class));
        verify(consumer, never()).ack();
    }

    @ParameterizedTest
    @EnumSource(value = CanalEntry.EventType.class, names = {"TRUNCATE", "ERASE", "RENAME"})
    void tableLevelDdlPurgesRegisteredGroupBeforeAcknowledging(CanalEntry.EventType type) {
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(recordOf(ddlEntry("users", type))));
        when(redis.execute(any(RedisScript.class), eq(List.of()), any(Object[].class)))
                .thenReturn(List.of("0", List.of("user:1", "user:2", "user:stats")));
        when(redis.unlink(List.of("user:1", "user:2"))).thenReturn(2L);

        assertThat(service.processBatch(consumer)).isTrue();

        var order = inOrder(redis, consumer);
        order.verify(redis).unlink(List.of("user:1", "user:2"));
        order.verify(consumer).ack();
        assertThat(metrics.get("canal.cache.purged").counter().count()).isEqualTo(2);
    }

    /** 2026-11-19 线上实测：Canal 对 RENAME 给出的是新表名，已注册表只出现在 SQL 里。 */
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "users_old|RENAME|RENAME TABLE users TO users_old",
            "_users_old|RENAME|RENAME TABLE `demo_db`.`users` TO `demo_db`.`_users_old`, `demo_db`.`_users_gho` TO `demo_db`.`users`",
            "orders|ERASE|DROP TABLE IF EXISTS orders, Users"})
    void tableLevelDdlPurgesRegisteredTableNamedOnlyInSql(String headerTable, CanalEntry.EventType type, String sql) {
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(recordOf(ddlEntry(headerTable, type, sql))));
        when(redis.execute(any(RedisScript.class), eq(List.of()), any(Object[].class)))
                .thenReturn(List.of("0", List.of("user:1", "user:2", "user:stats")));
        when(redis.unlink(List.of("user:1", "user:2"))).thenReturn(2L);

        assertThat(service.processBatch(consumer)).isTrue();

        verify(redis, times(1)).unlink(List.of("user:1", "user:2"));
        verify(consumer).ack();
        assertThat(metrics.get("canal.cache.purged").counter().count()).isEqualTo(2);
    }

    /** 设计文档第 6.6 节：别的库里同名的表被清空、删除时不清这边的缓存；SQL 里点名了本库的表时照清。 */
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "users|TRUNCATE|TRUNCATE TABLE users|false",
            "users|ERASE|DROP TABLE `other_db`.`users`|false",
            "users_old|RENAME|RENAME TABLE users TO users_old|false",
            "users_old|RENAME|RENAME TABLE `demo_db`.`users` TO `other_db`.`users_old`|true"})
    void ddlInAnotherDatabasePurgesOnlyWhenItNamesThisDatabase(String headerTable, CanalEntry.EventType type, String sql,
                                                               boolean purges) {
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(recordOf(ddlEntry("other_db", headerTable, type, sql))));
        when(redis.execute(any(RedisScript.class), eq(List.of()), any(Object[].class)))
                .thenReturn(List.of("0", List.of("user:1", "user:2")));
        when(redis.unlink(List.of("user:1", "user:2"))).thenReturn(2L);

        assertThat(service.processBatch(consumer)).isTrue();

        verify(redis, times(purges ? 1 : 0)).unlink(List.of("user:1", "user:2"));
        verify(consumer).ack();
    }

    @Test
    void heartbeatCountsOnlyAfterItsBatchIsAcknowledgedAndNeverTouchesTheCache() {
        CdcHeartbeatMonitor monitor = heartbeatMonitor();
        CanalCacheSyncService withHeartbeat = serviceWith(monitor);
        long beat = System.currentTimeMillis();
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(recordOf(heartbeatEntry(beat),
                rowEntry(CanalEntry.EventType.UPDATE, column("id", "1"), column("name", "a")))));
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class))).thenReturn(1L);
        doAnswer(invocation -> {
            assertThat(monitor.status().partitionAgeMs()).isEmpty();
            return null;
        }).when(consumer).ack();

        assertThat(withHeartbeat.processBatch(consumer)).isTrue();

        assertThat(monitor.status().partitionAgeMs()).containsOnlyKeys(0);
        assertThat(monitor.state()).isEqualTo(CdcHeartbeatMonitor.State.FRESH);
        verify(processor, never()).getConfig("cdc_heartbeat");
    }

    @Test
    void heartbeatInARolledBackBatchIsNotRecorded() {
        CdcHeartbeatMonitor monitor = heartbeatMonitor();
        CanalCacheSyncService withHeartbeat = serviceWith(monitor);
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(recordOf(heartbeatEntry(System.currentTimeMillis()),
                rowEntry(CanalEntry.EventType.UPDATE, column("id", "1"), column("name", "a")))));
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenThrow(new RedisConnectionFailureException("Redis unavailable"));

        assertThatThrownBy(() -> withHeartbeat.processBatch(consumer)).isInstanceOf(RedisConnectionFailureException.class);

        verify(consumer, never()).ack();
        assertThat(monitor.status().partitionAgeMs()).isEmpty();
        assertThat(monitor.cacheUsable()).isFalse();
    }

    @Test
    void heartbeatChainIsCheckedBeforeTheBatchIsAcknowledged() {
        CdcHeartbeatMonitor monitor = chainedHeartbeatMonitor();
        when(redis.execute(any(RedisScript.class), eq(List.of("test-chain")), any(Object[].class))).thenReturn(List.of());
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(recordOf(heartbeatUpdate(100, 200))));

        assertThat(serviceWith(monitor).processBatch(consumer)).isTrue();

        var order = inOrder(redis, consumer);
        order.verify(redis).execute(any(RedisScript.class), eq(List.of("test-chain")), eq("1"), eq("100"), eq("200"));
        order.verify(consumer).ack();
    }

    @Test
    void redisFailureWhileCheckingTheHeartbeatChainRollsTheBatchBack() {
        CdcHeartbeatMonitor monitor = chainedHeartbeatMonitor();
        when(redis.execute(any(RedisScript.class), eq(List.of("test-chain")), any(Object[].class)))
                .thenThrow(new RedisConnectionFailureException("Redis unavailable"));
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(recordOf(heartbeatUpdate(100, 200))));

        assertThatThrownBy(() -> serviceWith(monitor).processBatch(consumer))
                .isInstanceOf(RedisConnectionFailureException.class);

        verify(consumer, never()).ack();
        assertThat(monitor.status().lossEvents()).isZero();
    }

    /** 新增主键通过 Redis Pub/Sub 广播给接入方的布隆过滤器（设计文档第 6.3 节），在本批确认之前发出。 */
    @Test
    void insertedRowsAreBroadcastToQuerySideBloomFilters() {
        config.setBloomFilter(true);
        StringRedisTemplate pubSub = mock(StringRedisTemplate.class);
        ObjectMapper json = new ObjectMapper();
        CanalCacheSyncService withBloom = new CanalCacheSyncService(connector, new BinlogParser(),
                new FlatMessageBinlogParser(), processor, versions, mock(BigKeyDetector.class), redis, json, metrics,
                new TraceCollector(), new DeadLetterStore(redis, json, metrics),
                new CacheNamespacePurger(redis, new CacheBrowseService(redis)), CdcHeartbeatMonitor.disabled(),
                new BloomPublisher(pubSub, processor));
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class))).thenReturn(1L);
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(recordOf(
                rowEntry(CanalEntry.EventType.INSERT, column("id", "42"), column("name", "a")))));

        assertThat(withBloom.processBatch(consumer)).isTrue();

        var order = inOrder(pubSub, consumer);
        order.verify(pubSub).convertAndSend(CacheKeys.bloomChannel("demo_db", "users"), "42");
        order.verify(consumer).ack();
    }

    @Test
    void groupsWithoutBloomFilterAreNotBroadcast() {
        StringRedisTemplate pubSub = mock(StringRedisTemplate.class);

        new BloomPublisher(pubSub, processor).add("demo_db", "users", 42L);
        new BloomPublisher(pubSub, processor).add("demo_db", "orders", 42L);

        verifyNoInteractions(pubSub);
    }

    @Test
    void sameTableNameInAnotherDatabaseIsNotBroadcast() {
        config.setBloomFilter(true);
        StringRedisTemplate pubSub = mock(StringRedisTemplate.class);

        new BloomPublisher(pubSub, processor).add("other_db", "users", 42L);

        verifyNoInteractions(pubSub);
    }

    private CdcHeartbeatMonitor chainedHeartbeatMonitor() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(1);
        CdcHeartbeatMonitor monitor = new CdcHeartbeatMonitor(true, "cdc_heartbeat", 1, 1_000, 3_000, 1,
                jdbc, System::currentTimeMillis, null, redis, "test-chain");
        monitor.beat();
        return monitor;
    }

    private static CanalEntry.Entry heartbeatUpdate(long before, long after) {
        CanalEntry.RowData row = CanalEntry.RowData.newBuilder()
                .addBeforeColumns(column("id", "1")).addBeforeColumns(column("beat_ms", String.valueOf(before)))
                .addAfterColumns(column("id", "1")).addAfterColumns(column("beat_ms", String.valueOf(after))).build();
        return entry("cdc_heartbeat", CanalEntry.RowChange.newBuilder().setEventType(CanalEntry.EventType.UPDATE)
                .addRowDatas(row).build().toByteString());
    }

    private CdcHeartbeatMonitor heartbeatMonitor() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(1);
        CdcHeartbeatMonitor monitor = new CdcHeartbeatMonitor(true, "cdc_heartbeat", 1, 1_000, 3_000, 1,
                jdbc, System::currentTimeMillis, null);
        monitor.beat();
        return monitor;
    }

    private CanalCacheSyncService serviceWith(CdcHeartbeatMonitor monitor) {
        ObjectMapper json = new ObjectMapper();
        return new CanalCacheSyncService(connector, new BinlogParser(), new FlatMessageBinlogParser(), processor,
                versions, mock(BigKeyDetector.class), redis, json, metrics, new TraceCollector(),
                new DeadLetterStore(redis, json, metrics), new CacheNamespacePurger(redis, new CacheBrowseService(redis)),
                monitor);
    }

    @Test
    void ddlThatKeepsRowsOrTargetsUnregisteredTableDoesNotTouchRedis() {
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(recordOf(
                ddlEntry("users", CanalEntry.EventType.ALTER), ddlEntry("orders", CanalEntry.EventType.TRUNCATE))));

        assertThat(service.processBatch(consumer)).isTrue();

        verify(consumer).ack();
        verifyNoInteractions(redis);
    }

    @Test
    void retryDelayDoublesUpToThirtySeconds() {
        assertThat(List.of(1, 2, 3, 4, 5, 6, 50).stream().map(CanalCacheSyncService::retryDelayMs))
                .containsExactly(1_000L, 2_000L, 4_000L, 8_000L, 16_000L, 30_000L, 30_000L);
    }

    @Test
    void filteredDdlBatchIsAcknowledged() {
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(record(CanalEntry.EventType.CREATE)));

        assertThat(service.processBatch(consumer)).isTrue();

        verify(consumer).ack();
        verifyNoInteractions(redis);
        assertThat(service.getStatus().lastSuccessAt()).isNotNull();
    }

    @Test
    void insertInvalidatesBeforeAcknowledging() {
        config.setEnableVersionControl(false);
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(record(CanalEntry.EventType.INSERT)));
        when(redis.delete("user:1")).thenReturn(true);

        assertThat(service.processBatch(consumer)).isTrue();

        var order = inOrder(redis, consumer);
        order.verify(redis).delete("user:1");
        order.verify(consumer).ack();
        verify(redis, never()).execute(any(RedisScript.class), anyList(), any(Object[].class));
    }

    @Test
    void emptyPollIsNotAcknowledged() {
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of());

        assertThat(service.processBatch(consumer)).isFalse();

        verify(consumer, never()).ack();
        assertThat(service.getStatus().lastSuccessAt()).isNull();
    }

    @Test
    void failedAcknowledgmentDoesNotRecordSuccessfulBatch() {
        when(consumer.getRecordsWithoutAck()).thenReturn(List.of(record(CanalEntry.EventType.CREATE)));
        doThrow(new IllegalStateException("commit failed")).when(consumer).ack();

        assertThatThrownBy(() -> service.processBatch(consumer)).isInstanceOf(IllegalStateException.class);
        assertThat(service.getStatus().lastSuccessAt()).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"INSERT", "UPDATE"})
    void flatMessageWithMqFileNameDeletesCacheThenAcknowledges(String eventType) {
        when(connector.isFlatMessageMode()).thenReturn(true);
        FlatMessage message = new FlatMessage();
        message.setType(eventType);
        message.setTable("users");
        message.setIsDdl(false);
        message.setData(List.of(Map.of("id", "1", "name", "new")));
        when(consumer.getFlatMessagesWithoutAck()).thenReturn(List.of(message));
        when(redis.delete("user:1")).thenReturn(true);

        assertThat(service.processBatch(consumer)).isTrue();

        var order = inOrder(redis, consumer);
        order.verify(redis).delete("user:1");
        order.verify(consumer).ack();
    }

    private KafkaCanalRecord record(CanalEntry.EventType type) {
        return recordOf(rowEntry(type, column("id", "1")));
    }

    private double deadLettered(String reason) {
        var counter = metrics.find(DeadLetterStore.METRIC).tag("reason", reason).counter();
        return counter == null ? 0 : counter.count();
    }

    private static KafkaCanalRecord recordOf(CanalEntry.Entry... entries) {
        return new KafkaCanalRecord("example", 0, 1L, new Message(1L, List.of(entries)));
    }

    private static CanalEntry.Column column(String name, String value) {
        return CanalEntry.Column.newBuilder().setName(name).setValue(value).build();
    }

    private static CanalEntry.Entry rowEntry(CanalEntry.EventType type, CanalEntry.Column... columns) {
        CanalEntry.RowData.Builder row = CanalEntry.RowData.newBuilder();
        for (CanalEntry.Column column : columns) {
            row.addBeforeColumns(column).addAfterColumns(column);
        }
        return entry("users", CanalEntry.RowChange.newBuilder().setEventType(type).addRowDatas(row).build().toByteString());
    }

    private static CanalEntry.Entry ddlEntry(String table, CanalEntry.EventType type) {
        return ddlEntry(table, type, type + " TABLE " + table);
    }

    private static CanalEntry.Entry ddlEntry(String table, CanalEntry.EventType type, String sql) {
        return ddlEntry("demo_db", table, type, sql);
    }

    private static CanalEntry.Entry ddlEntry(String schema, String table, CanalEntry.EventType type, String sql) {
        return entry(schema, table, CanalEntry.RowChange.newBuilder().setEventType(type).setIsDdl(true)
                .setSql(sql).build().toByteString());
    }

    private static CanalEntry.Entry heartbeatEntry(long beatMs) {
        CanalEntry.RowData row = CanalEntry.RowData.newBuilder()
                .addAfterColumns(column("id", "1")).addAfterColumns(column("beat_ms", String.valueOf(beatMs))).build();
        return entry("cdc_heartbeat", CanalEntry.RowChange.newBuilder().setEventType(CanalEntry.EventType.UPDATE)
                .addRowDatas(row).build().toByteString());
    }

    private static CanalEntry.Entry malformedEntry() {
        return entry("users", ByteString.copyFrom(new byte[]{10, 5, 1}));
    }

    private static CanalEntry.Entry entry(String table, ByteString storeValue) {
        return entry("demo_db", table, storeValue);
    }

    private static CanalEntry.Entry entry(String schema, String table, ByteString storeValue) {
        return CanalEntry.Entry.newBuilder()
                .setHeader(CanalEntry.Header.newBuilder().setSchemaName(schema).setTableName(table)
                        .setLogfileName("mysql-bin.000001").setLogfileOffset(100L))
                .setEntryType(CanalEntry.EntryType.ROWDATA).setStoreValue(storeValue).build();
    }
}
