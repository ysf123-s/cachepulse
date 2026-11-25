package com.cachepulse.archive;

import com.cachepulse.common.event.FieldChange;
import com.cachepulse.common.model.BinlogEvent;


import java.util.List;

public record ArchivedChange(Summary summary, BinlogEvent event, String topic, int partition,
                             String offset, List<FieldChange> changes, Recovery recovery) {
    public record Summary(String sequence, String eventId, String sourceId, String database,
                          String table, String operation, String primaryKey, String primaryKeyValue,
                          String beforePrimaryKeyValue, long eventTimeMillis, String archivedAt,
                          String schemaSnapshotId) { }

    public record Recovery(boolean available, String reason) { }

    public record Page(List<Summary> items, String nextCursor) { }

    public record Filter(Long before, String table, String key, String operation,
                         Long from, Long to, int limit) { }
}
