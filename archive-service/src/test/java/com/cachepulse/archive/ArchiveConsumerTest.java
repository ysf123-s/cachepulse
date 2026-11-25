package com.cachepulse.archive;

import com.cachepulse.common.kafka.CanalConnectorWrapper;
import com.cachepulse.common.model.KafkaCanalRecord;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.sql.Types;

import static com.cachepulse.archive.ArchiveTestFixture.column;
import static com.cachepulse.archive.ArchiveTestFixture.inserted;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ArchiveConsumerTest {
    @Test
    void successfulArchivePrecedesAckAndEmptyPollDoesNotAck() throws Exception {
        ArchiveTestFixture fixture = new ArchiveTestFixture();
        CanalConnectorWrapper.PartitionConsumer kafka = mock(CanalConnectorWrapper.PartitionConsumer.class);
        KafkaCanalRecord record = fixture.record(10, fixture.rows(CanalEntry.EventType.INSERT, inserted("1", "new")));
        when(kafka.getRecordsWithoutAck()).thenReturn(List.of(record), List.of());
        doAnswer(call -> { assertThat(fixture.count()).isEqualTo(1); return null; }).when(kafka).ack();
        ArchiveConsumer consumer = new ArchiveConsumer(mock(CanalConnectorWrapper.class), fixture.service, "archive");
        assertThat(consumer.processBatch(kafka)).isTrue();
        assertThat(consumer.processBatch(kafka)).isFalse();
        verify(kafka, times(1)).ack();
        assertThat(consumer.status().acknowledgedNextOffsets()).containsEntry("example:2", "11");
    }

    @Test
    void databaseFailureDoesNotAckOrAdvanceSuccessState() {
        CanalConnectorWrapper.PartitionConsumer kafka = mock(CanalConnectorWrapper.PartitionConsumer.class);
        ArchiveService archive = mock(ArchiveService.class);
        KafkaCanalRecord record = mock(KafkaCanalRecord.class);
        when(kafka.getRecordsWithoutAck()).thenReturn(List.of(record));
        when(archive.archiveBatch(List.of(record))).thenThrow(new IllegalStateException("database unavailable"));
        ArchiveConsumer consumer = new ArchiveConsumer(mock(CanalConnectorWrapper.class), archive, "archive");
        assertThatThrownBy(() -> consumer.processBatch(kafka)).hasMessage("database unavailable");
        verify(kafka, never()).ack();
        assertThat(consumer.status().lastSuccessAt()).isNull();
        assertThat(consumer.status().acknowledgedNextOffsets()).isEmpty();
    }

    @Test
    void databaseCommitFollowedByAckFailureCanReplayWithoutDuplicatingStoredRows() throws Exception {
        ArchiveTestFixture fixture = new ArchiveTestFixture();
        CanalConnectorWrapper.PartitionConsumer kafka = mock(CanalConnectorWrapper.PartitionConsumer.class);
        when(kafka.getRecordsWithoutAck()).thenReturn(List.of(fixture.record(10,
                fixture.rows(CanalEntry.EventType.INSERT, inserted("1", "committed")))));
        doThrow(new IllegalStateException("commit failed")).doNothing().when(kafka).ack();
        ArchiveConsumer consumer = new ArchiveConsumer(mock(CanalConnectorWrapper.class), fixture.service, "archive");
        assertThatThrownBy(() -> consumer.processBatch(kafka)).hasMessage("commit failed");
        assertThat(fixture.count()).isEqualTo(1);
        assertThat(consumer.status().lastSuccessAt()).isNull();
        assertThat(consumer.processBatch(kafka)).isTrue();
        assertThat(fixture.count()).isEqualTo(1);
        assertThat(consumer.status().acknowledgedDuplicateRows()).isEqualTo(1);
    }

    /** Delivery tasks are no longer written by the archive (design 6.4); DeliveryConsumerTest covers their rollback. */
    @Test
    void laterRecordFailureRollsBackWholePollBeforeSuccessfulReplay() throws Exception {
        ArchiveTestFixture fixture = new ArchiveTestFixture();
        KafkaCanalRecord first = fixture.record(10, fixture.rows(CanalEntry.EventType.INSERT, inserted("1", "first")));
        CanalEntry.RowData missingKey = CanalEntry.RowData.newBuilder()
                .addAfterColumns(column("name", "broken", 1, Types.VARCHAR, "varchar(64)", false)).build();
        KafkaCanalRecord broken = fixture.record(11, fixture.rows(CanalEntry.EventType.INSERT, missingKey));
        KafkaCanalRecord second = fixture.record(11, fixture.rows(CanalEntry.EventType.INSERT, inserted("2", "second")));
        CanalConnectorWrapper.PartitionConsumer kafka = mock(CanalConnectorWrapper.PartitionConsumer.class);
        when(kafka.getRecordsWithoutAck()).thenReturn(List.of(first, broken), List.of(first, second), List.of(first, second));
        ArchiveConsumer consumer = new ArchiveConsumer(mock(CanalConnectorWrapper.class), fixture.service, "archive");

        assertThatThrownBy(() -> consumer.processBatch(kafka)).hasMessageContaining("primary key");
        assertThat(fixture.count()).isZero();
        verify(kafka, never()).ack();
        assertThat(consumer.status().acknowledgedNextOffsets()).isEmpty();

        assertThat(consumer.processBatch(kafka)).isTrue();
        assertThat(consumer.processBatch(kafka)).isTrue();
        assertThat(fixture.count()).isEqualTo(2);
        assertThat(consumer.status().acknowledgedNewRows()).isEqualTo(2);
        assertThat(consumer.status().acknowledgedDuplicateRows()).isEqualTo(2);
        assertThat(consumer.status().acknowledgedNextOffsets()).containsEntry("example:2", "12");
        verify(kafka, times(2)).ack();
    }
}
