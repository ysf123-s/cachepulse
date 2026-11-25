package com.cachepulse.delivery;

import com.cachepulse.common.kafka.CanalConnectorWrapper;
import com.cachepulse.common.model.KafkaCanalRecord;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;

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
    void laterRecordFailureRollsBackSourceEventsAndTasksOfTheWholePoll() {
        f.subscribe("sub", "first", "INSERT", true);
        KafkaCanalRecord first = f.record(10, f.rows(CanalEntry.EventType.INSERT, inserted("1", "first")));
        CanalEntry.RowData missingKey = CanalEntry.RowData.newBuilder()
                .addAfterColumns(column("name", "broken", 1, Types.VARCHAR, "varchar(64)", false)).build();
        KafkaCanalRecord broken = f.record(11, f.rows(CanalEntry.EventType.INSERT, missingKey));
        KafkaCanalRecord second = f.record(11, f.rows(CanalEntry.EventType.INSERT, inserted("2", "second")));
        CanalConnectorWrapper.PartitionConsumer kafka = mock(CanalConnectorWrapper.PartitionConsumer.class);
        when(kafka.getRecordsWithoutAck()).thenReturn(List.of(first, broken), List.of(first, second));

        assertThatThrownBy(() -> f.consumer.processBatch(kafka)).hasMessageContaining("primary key");
        assertThat(f.sourceEventCount()).isZero();
        assertThat(f.taskCount()).isZero();
        verify(kafka, never()).ack();
        assertThat(f.consumer.status().acknowledgedNextOffsets()).isEmpty();

        assertThat(f.consumer.processBatch(kafka)).isTrue();
        verify(kafka).ack();
        assertThat(f.taskCount()).isEqualTo(2);
        assertThat(f.consumer.status().createdTasks()).isEqualTo(2);
        assertThat(f.consumer.status().acknowledgedNextOffsets()).containsEntry("example:2", "12");
    }

    @Test
    void unregisteredTargetFailsThePollInsteadOfDroppingTheTask() {
        f.subscribe("sub", "retired", "INSERT", true);
        assertThatThrownBy(() -> f.consumer.handle(List.of(f.record(10, f.rows(CanalEntry.EventType.INSERT, inserted("1", "x"))))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("retired");
        assertThat(f.sourceEventCount()).isZero();
    }
}
