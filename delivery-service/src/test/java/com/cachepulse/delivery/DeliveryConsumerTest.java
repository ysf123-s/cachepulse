package com.cachepulse.delivery;

import com.cachepulse.common.kafka.CanalConnectorWrapper;
import com.cachepulse.common.model.KafkaCanalRecord;
import com.cachepulse.common.parser.BinlogParser;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import java.sql.Types;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.cachepulse.delivery.DeliveryTestFixture.column;
import static com.cachepulse.delivery.DeliveryTestFixture.inserted;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/** The delivery service's own consumer group: matching, task creation and source_event dedup (design 6.4). */
class DeliveryConsumerTest {
    private DeliveryTestFixture f;

    @BeforeEach
    void setUp() throws Exception {
        f = new DeliveryTestFixture();
        f.target("first", "http://localhost/first");
        f.target("second", "http://localhost/second");
    }

    @Test
    void sameBinlogRowRedeliveredWithDifferentKafkaOffsetsCreatesExactlyOneTask() {
        f.subscribe("sub", "first", "INSERT", true);
        var entry = f.rows(CanalEntry.EventType.INSERT, inserted("1", "once"));
        KafkaCanalRecord original = f.record(100, entry);
        String originalEventId = f.eventIds(original).get(0);

        DeliveryConsumer.Result first = f.consumer.handle(List.of(original));
        DeliveryConsumer.Result republished = f.consumer.handle(List.of(f.record(200, entry)));
        DeliveryConsumer.Result moved = f.consumer.handle(List.of(new KafkaCanalRecord("example", 5, 7, original.message())));

        assertThat(first.newEvents()).isEqualTo(1);
        assertThat(first.tasks()).isEqualTo(1);
        assertThat(republished.duplicates()).isEqualTo(1);
        assertThat(republished.tasks()).isZero();
        assertThat(moved.duplicates()).isEqualTo(1);
        assertThat(f.sourceEventCount()).isEqualTo(1);
        assertThat(f.store.listDeliveryTasks(null, "sub", null, 10)).singleElement()
                .satisfies(task -> assertThat(task.eventId()).isEqualTo(originalEventId));
        assertThat(f.jdbc.queryForObject("SELECT event_id FROM source_event", String.class)).isEqualTo(originalEventId);
    }

    @Test
    void matchingEventCreatesOneTaskPerEnabledSubscription() {
        f.subscribe("sub-a", "first", "INSERT,UPDATE", true);
        f.subscribe("sub-b", "second", "INSERT", true);
        f.subscribe("sub-disabled", "first", "INSERT", false);
        f.subscribe("sub-delete-only", "second", "DELETE", true);

        DeliveryConsumer.Result result = f.consumer.handle(List.of(f.record(10, f.rows(CanalEntry.EventType.INSERT, inserted("1", "fan-out")))));

        assertThat(result.tasks()).isEqualTo(2);
        assertThat(f.taskCount()).isEqualTo(2);
        assertThat(f.store.listDeliveryTasks(null, "sub-a", null, 10)).singleElement().satisfies(task -> {
            assertThat(task.targetUrl()).isEqualTo("http://localhost/first");
            assertThat(task.roundType()).isEqualTo("INITIAL");
            assertThat(task.status()).isEqualTo("PENDING");
            assertThat(f.store.decode(task.eventPayload(), java.util.Map.class)).containsEntry("tableName", "inventory");
        });
        assertThat(f.store.listDeliveryTasks(null, "sub-b", null, 10)).hasSize(1);
        assertThat(f.store.listDeliveryTasks(null, "sub-disabled", null, 10)).isEmpty();
        assertThat(f.store.listDeliveryTasks(null, "sub-delete-only", null, 10)).isEmpty();
    }

    @Test
    void eventsWithoutMatchingSubscriptionAreStillClaimedAndOtherTablesAreIgnored() {
        CanalEntry.RowChange change = CanalEntry.RowChange.newBuilder().setEventType(CanalEntry.EventType.INSERT)
                .addRowDatas(inserted("1", "nobody listens")).build();
        DeliveryConsumer.Result result = f.consumer.handle(List.of(f.record(10,
                f.entry("inventory", change), f.entry("not_registered", change))));
        assertThat(result.newEvents()).isEqualTo(1);
        assertThat(result.tasks()).isZero();
        assertThat(f.sourceEventCount()).isEqualTo(1);
    }

    @Test
    void looksUpSubscriptionsOncePerTableAndPollIncludingEmptyResults() {
        DeliveryStore store = spy(f.store);
        DeliveryConsumer consumer = new DeliveryConsumer(mock(CanalConnectorWrapper.class), new BinlogParser(),
                f.properties, f.tables, store);
        String source = f.properties.getSourceId();
        String database = f.properties.getDatabase();
        KafkaCanalRecord first = f.record(10, f.rows(CanalEntry.EventType.INSERT, inserted("1", "first")));
        KafkaCanalRecord second = f.record(11, f.rows(CanalEntry.EventType.INSERT, inserted("2", "second")));

        DeliveryConsumer.Result unsubscribed = consumer.handle(List.of(first, second));

        assertThat(unsubscribed.newEvents()).isEqualTo(2);
        assertThat(unsubscribed.tasks()).isZero();
        verify(store).findEnabledSubscriptions(source, database, "inventory");

        f.subscribe("new-sub", "first", "INSERT", true);
        KafkaCanalRecord third = f.record(12, f.rows(CanalEntry.EventType.INSERT, inserted("3", "third")));
        KafkaCanalRecord fourth = f.record(13, f.rows(CanalEntry.EventType.INSERT, inserted("4", "fourth")));

        DeliveryConsumer.Result subscribed = consumer.handle(List.of(second, third, fourth));

        assertThat(subscribed.duplicates()).isEqualTo(1);
        assertThat(subscribed.tasks()).isEqualTo(2);
        assertThat(f.sourceEventCount()).isEqualTo(4);
        verify(store, times(2)).findEnabledSubscriptions(source, database, "inventory");
    }

    @Test
    void databaseFailureRollsBackSourceEventsAndTasksOfTheWholePoll() {
        f.subscribe("sub", "first", "INSERT", true);
        KafkaCanalRecord first = f.record(10, f.rows(CanalEntry.EventType.INSERT, inserted("1", "first")));
        KafkaCanalRecord second = f.record(11, f.rows(CanalEntry.EventType.INSERT, inserted("2", "second")));
        DeliveryStore store = spy(f.store);
        AtomicInteger claims = new AtomicInteger();
        doAnswer(call -> {
            if (claims.incrementAndGet() == 2) throw new DataAccessResourceFailureException("database unavailable");
            return call.callRealMethod();
        }).when(store).claimSourceEvent(anyString(), anyString());
        DeliveryConsumer consumer = new DeliveryConsumer(mock(CanalConnectorWrapper.class), new BinlogParser(),
                f.properties, f.tables, store);
        CanalConnectorWrapper.PartitionConsumer kafka = mock(CanalConnectorWrapper.PartitionConsumer.class);
        when(kafka.getRecordsWithoutAck()).thenReturn(List.of(first, second));

        assertThatThrownBy(() -> consumer.processBatch(kafka)).hasMessageContaining("database unavailable");
        assertThat(f.sourceEventCount()).isZero();
        assertThat(f.taskCount()).isZero();
        verify(kafka, never()).ack();
        assertThat(consumer.status().acknowledgedNextOffsets()).isEmpty();

        assertThat(consumer.processBatch(kafka)).isTrue();
        verify(kafka).ack();
        assertThat(f.taskCount()).isEqualTo(2);
        assertThat(consumer.status().createdTasks()).isEqualTo(2);
        assertThat(consumer.status().acknowledgedNextOffsets()).containsEntry("example:2", "12");
    }

    @Test
    void rowsThatCanNeverBeProcessedAreRejectedAndTheRestOfThePollIsDelivered() {
        // 缺主键的行、解析不了的 Entry、不是 Canal Message 的记录：重试多少次结果都一样，以前会让整个分区永远卡住
        f.subscribe("sub", "first", "INSERT", true);
        CanalEntry.RowData missingKey = CanalEntry.RowData.newBuilder()
                .addAfterColumns(column("name", "broken", 1, Types.VARCHAR, "varchar(64)", false)).build();
        CanalEntry.Entry malformed = CanalEntry.Entry.newBuilder().setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(CanalEntry.Header.newBuilder().setSchemaName(f.properties.getDatabase()).setTableName("inventory")
                        .setLogfileName("mysql-bin.000001").setLogfileOffset(9000))
                .setStoreValue(ByteString.copyFrom(new byte[]{10, 5, 1})).build();
        KafkaCanalRecord broken = f.record(10, f.rows(CanalEntry.EventType.INSERT, missingKey, inserted("1", "same entry")));
        KafkaCanalRecord mixed = f.record(11, malformed, f.rows(CanalEntry.EventType.INSERT, inserted("2", "after bad entry")));
        KafkaCanalRecord notDecoded = new KafkaCanalRecord("example", 2, 12, null);
        CanalConnectorWrapper.PartitionConsumer kafka = mock(CanalConnectorWrapper.PartitionConsumer.class);
        when(kafka.getRecordsWithoutAck()).thenReturn(List.of(broken, mixed, notDecoded));

        assertThat(f.consumer.processBatch(kafka)).isTrue();

        verify(kafka).ack();
        assertThat(f.taskCount()).isEqualTo(2);
        assertThat(f.consumer.status().acknowledgedRejectedEvents()).isEqualTo(3);
        assertThat(f.jdbc.queryForList("SELECT kafka_offset FROM rejected_event ORDER BY kafka_offset", Long.class))
                .containsExactly(10L, 11L, 12L);
        assertThat(f.jdbc.queryForObject("SELECT reason FROM rejected_event WHERE kafka_offset=10", String.class))
                .contains("primary key");
        assertThat(f.jdbc.queryForObject("SELECT binlog_position FROM rejected_event WHERE kafka_offset=11", Long.class))
                .isEqualTo(9000L);

        // 同一位置重复投递（确认失败后重拉）只记一次
        assertThat(f.consumer.handle(List.of(broken, mixed, notDecoded)).rejected()).isZero();
        assertThat(f.jdbc.queryForObject("SELECT COUNT(*) FROM rejected_event", Long.class)).isEqualTo(3);
    }

    @Test
    void unregisteredTargetIsRejectedForThatSubscriptionOnly() {
        f.subscribe("sub-retired", "retired", "INSERT", true);
        f.subscribe("sub-live", "first", "INSERT", true);
        KafkaCanalRecord record = f.record(10, f.rows(CanalEntry.EventType.INSERT, inserted("1", "x")));

        DeliveryConsumer.Result result = f.consumer.handle(List.of(record));

        assertThat(result.tasks()).isEqualTo(1);
        assertThat(result.rejected()).isEqualTo(1);
        assertThat(f.sourceEventCount()).isEqualTo(1);
        assertThat(f.store.listDeliveryTasks(null, "sub-live", null, 10)).hasSize(1);
        var rejected = f.jdbc.queryForMap("SELECT event_id, subscription_id, reason FROM rejected_event");
        assertThat(rejected.get("event_id")).isEqualTo(f.eventIds(record).get(0));
        assertThat(rejected.get("subscription_id").toString().trim()).isEqualTo("sub-retired");
        assertThat(rejected.get("reason").toString()).contains("retired");
    }
}
