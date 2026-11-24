package com.example.canalcache.platform;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.example.canalcache.model.KafkaCanalRecord;
import com.example.canalcache.platform.history.ArchiveService;
import com.example.canalcache.platform.history.EventIdentity;
import com.example.canalcache.parser.BinlogParser;
import com.example.canalcache.platform.subscription.EventSubscription;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.example.canalcache.platform.ArchiveTestFixture.*;
import static org.assertj.core.api.Assertions.*;

class ArchiveSourceDedupTest {
    private ArchiveTestFixture fixture;

    @BeforeEach
    void setUp() throws Exception {
        fixture = new ArchiveTestFixture();
        PlatformProperties.TargetConfig target = new PlatformProperties.TargetConfig();
        target.setUrl("http://internal-service/webhook");
        fixture.properties.getTargets().put("hook", target);
        fixture.store.saveSubscription(new EventSubscription("dedup-sub", fixture.properties.getSourceId(),
                fixture.properties.getDatabase(), "inventory", "hook", "INSERT,UPDATE,DELETE",
                null, null, null, true, 1, null, null));
    }

    @Test
    void republishedSourceEventKeepsOneHistoryAndNotificationWithTheOriginalEventId() {
        var entry = fixture.rows(CanalEntry.EventType.INSERT, inserted("1", "first"));
        var original = fixture.record(100, entry);
        String eventId = EventIdentity.of(fixture.properties.getSourceId(), original, new BinlogParser().parse(List.of(entry)).get(0));

        assertThat(fixture.service.archive(original).inserted()).isEqualTo(1);
        assertThat(fixture.service.archive(fixture.record(200, entry)).duplicates()).isEqualTo(1);
        assertThat(fixture.service.archive(original).duplicates()).isEqualTo(1);
        assertThat(fixture.count()).isEqualTo(1);
        assertThat(fixture.store.listDeliveryTasks(null, "dedup-sub", null, 10)).singleElement()
                .satisfies(task -> assertThat(task.eventId()).isEqualTo(eventId));
        assertThat(fixture.jdbc.queryForObject("SELECT event_id FROM change_event", String.class)).isEqualTo(eventId);
        assertThat(fixture.jdbc.queryForObject("SELECT kafka_offset FROM change_event", Long.class)).isEqualTo(100L);
    }

    @Test
    void sourceRowsSurviveCanalSplittingRegroupingAndDifferentKafkaCoordinates() {
        var first = inserted("1", "one");
        var second = inserted("2", "two");
        var entry = fixture.rows(CanalEntry.EventType.INSERT, first, second);
        assertThat(fixture.service.archive(fixture.record(100, entry)).inserted()).isEqualTo(2);
        var ignored = fixture.entry("unregistered", CanalEntry.RowChange.newBuilder()
                .setEventType(CanalEntry.EventType.INSERT).addRowDatas(inserted("9", "ignored")).build());
        var republished = new KafkaCanalRecord("republished", 5, 900,
                new Message(200L, List.of(ignored, singleRow(entry, second), singleRow(entry, first))));

        ArchiveService.Result result = fixture.service.archive(republished);
        assertThat(result.duplicates()).isEqualTo(2);
        assertThat(result.filtered()).isEqualTo(1);
        assertThat(fixture.count()).isEqualTo(2);
        assertThat(fixture.store.listDeliveryTasks(null, "dedup-sub", null, 10)).hasSize(2);
    }

    @Test
    void repeatedChangesOfTheSamePrimaryKeyWithinOneBinlogEventRemainDistinct() {
        var first = updated("1", "A", "B");
        var second = updated("1", "B", "C");
        var entry = fixture.rows(CanalEntry.EventType.UPDATE, first, second);
        assertThat(fixture.service.archive(fixture.record(100, entry)).inserted()).isEqualTo(2);
        var replay = fixture.service.archive(fixture.record(200, singleRow(entry, first), singleRow(entry, second)));
        assertThat(replay.duplicates()).isEqualTo(2);
        assertThat(fixture.count()).isEqualTo(2);
        assertThat(fixture.jdbc.queryForList("SELECT source_event_key FROM change_event", String.class)).doesNotHaveDuplicates();
        assertThat(fixture.store.listDeliveryTasks(null, "dedup-sub", null, 10)).hasSize(2);
    }

    @Test
    void changesAtDifferentSourcePositionsDoNotCollapseEvenForTheSamePrimaryKeyAndValues() {
        var first = fixture.rows(CanalEntry.EventType.UPDATE, updated("1", "A", "B"));
        var later = fixture.rows(CanalEntry.EventType.UPDATE, updated("1", "A", "B"));
        assertThat(fixture.service.archive(fixture.record(100, first)).inserted()).isEqualTo(1);
        assertThat(fixture.service.archive(fixture.record(200, later)).inserted()).isEqualTo(1);
        assertThat(fixture.count()).isEqualTo(2);
    }

    @Test
    void anExistingLegacyCopyAndItsCanonicalSourceOwnerAreBothRecognizedAsDuplicates() {
        var entry = fixture.rows(CanalEntry.EventType.INSERT, inserted("1", "legacy"));
        fixture.service.archive(fixture.record(100, entry));
        var oldCopy = fixture.record(200, entry);
        String oldCopyId = EventIdentity.of(fixture.properties.getSourceId(), oldCopy, new BinlogParser().parse(List.of(entry)).get(0));
        fixture.jdbc.update("INSERT INTO change_event (event_id,source_event_key,source_id,database_name,table_name,operation,"
                + "primary_key_name,primary_key_value,before_primary_key_value,event_time_ms,schema_snapshot_id,"
                + "kafka_topic,kafka_partition,kafka_offset,entry_index,row_index,event_json) "
                + "SELECT ?,NULL,source_id,database_name,table_name,operation,primary_key_name,primary_key_value,"
                + "before_primary_key_value,event_time_ms,schema_snapshot_id,kafka_topic,kafka_partition,200,entry_index,row_index,event_json "
                + "FROM change_event", oldCopyId);

        assertThat(fixture.service.archive(oldCopy).duplicates()).isEqualTo(1);
        assertThat(fixture.service.archive(fixture.record(300, entry)).duplicates()).isEqualTo(1);
        assertThat(fixture.count()).isEqualTo(2);
        assertThat(fixture.jdbc.queryForObject("SELECT COUNT(*) FROM change_event WHERE source_event_key IS NULL", Long.class)).isEqualTo(1);
        assertThat(fixture.store.find(oldCopyId, fixture.properties)).isNotNull();
        assertThat(fixture.store.listDeliveryTasks(null, "dedup-sub", null, 10)).hasSize(1);
    }

    @Test
    void concurrentRepublicationsAreArbitratedByTheDatabaseUniqueKey() throws Exception {
        var entry = fixture.rows(CanalEntry.EventType.INSERT, inserted("1", "concurrent"));
        var executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            var first = executor.submit(() -> { start.await(); return fixture.service.archive(fixture.record(100, entry)); });
            var second = executor.submit(() -> { start.await(); return fixture.service.archive(fixture.record(200, entry)); });
            start.countDown();
            var a = first.get(10, TimeUnit.SECONDS);
            var b = second.get(10, TimeUnit.SECONDS);
            assertThat(a.inserted() + b.inserted()).isEqualTo(1);
            assertThat(a.duplicates() + b.duplicates()).isEqualTo(1);
            assertThat(fixture.count()).isEqualTo(1);
            assertThat(fixture.store.listDeliveryTasks(null, "dedup-sub", null, 10)).hasSize(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void missingSourcePositionFailsWithoutStoringHistoryOrTasks() {
        var entry = fixture.rows(CanalEntry.EventType.INSERT, inserted("1", "invalid"));
        var invalid = entry.toBuilder().setHeader(entry.getHeader().toBuilder().clearLogfileName().setLogfileOffset(0)).build();
        assertThatThrownBy(() -> fixture.service.archive(fixture.record(100, invalid)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("binlog");
        assertThat(fixture.count()).isZero();
        assertThat(fixture.store.listDeliveryTasks(null, "dedup-sub", null, 10)).isEmpty();
    }

    private static CanalEntry.Entry singleRow(CanalEntry.Entry original, CanalEntry.RowData row) {
        try {
            var change = CanalEntry.RowChange.parseFrom(original.getStoreValue()).toBuilder().clearRowDatas().addRowDatas(row);
            return original.toBuilder().setStoreValue(change.build().toByteString()).build();
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    private static CanalEntry.RowData updated(String id, String before, String after) {
        return CanalEntry.RowData.newBuilder()
                .addBeforeColumns(column("id", id, 0, Types.BIGINT, "bigint", true))
                .addBeforeColumns(column("name", before, 1, Types.VARCHAR, "varchar(64)", false))
                .addAfterColumns(column("id", id, 0, Types.BIGINT, "bigint", true))
                .addAfterColumns(column("name", after, 1, Types.VARCHAR, "varchar(64)", false)).build();
    }
}
