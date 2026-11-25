package com.cachepulse.archive;

import com.cachepulse.common.event.EventIdentity;
import com.cachepulse.common.event.FieldChange;
import com.cachepulse.common.model.BinlogEvent;
import com.cachepulse.common.model.KafkaCanalRecord;
import com.cachepulse.common.parser.BinlogParser;
import com.cachepulse.common.platform.PlatformProperties;
import com.cachepulse.common.schema.TableSchema;

import org.springframework.transaction.support.TransactionTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArchiveService {
    private final PlatformProperties properties;
    private final TableRegistry tables;
    private final BinlogParser parser;
    private final ArchiveStore store;
    private final TransactionTemplate transaction;

    public ArchiveService(PlatformProperties properties, TableRegistry tables, BinlogParser parser,
                           ArchiveStore store, TransactionTemplate transaction) {
        this.properties = properties;
        this.tables = tables;
        this.parser = parser;
        this.store = store;
        this.transaction = transaction;
    }

    public Result archive(KafkaCanalRecord record) {
        return archiveBatch(List.of(record));
    }

    /** One Kafka poll shares a local archive transaction, independent of source MySQL transaction boundaries. */
    public Result archiveBatch(List<KafkaCanalRecord> records) {
        return transaction.execute(status -> {
            int inserted = 0, duplicate = 0, filtered = 0, ddl = 0;
            for (KafkaCanalRecord record : records) {
                if (record.message() == null || record.message().isRaw()) {
                    throw new IllegalArgumentException("Expected a decoded Canal Message at " + record.topic() + ":"
                            + record.partition() + ":" + record.offset());
                }
                List<BinlogEvent> events = parser.parseIncludingDdl(record.message().getEntries());
                Map<String, Integer> sourceRowOccurrences = new HashMap<>();
                for (BinlogEvent event : events) {
                    if (event.isDdl() && properties.getDatabase().equals(event.getDatabaseName())) {
                        store.invalidateSchemasForDdl(properties.getSourceId(), properties.getDatabase(), event,
                                event.getEventType() + " at " + record.topic() + ":" + record.partition() + ":" + record.offset());
                        ddl++;
                        continue;
                    }
                    TableSchema schema = tables.get(event.getDatabaseName(), event.getTableName());
                    if (schema == null) { filtered++; continue; }
                    Map<String, Object> current = event.getAfterData() == null ? event.getBeforeData() : event.getAfterData();
                    String key = primaryKey(current, schema);
                    String beforeKey = event.getBeforeData() == null ? null : primaryKey(event.getBeforeData(), schema);
                    String eventId = EventIdentity.of(properties.getSourceId(), record, event);
                    String sourceRowKey = EventIdentity.sourceRowKey(properties.getSourceId(), event, key, beforeKey);
                    int occurrence = sourceRowOccurrences.merge(sourceRowKey, 1, Integer::sum) - 1;
                    String sourceEventKey = EventIdentity.sourceEventKey(sourceRowKey, occurrence);
                    if (store.insert(eventId, sourceEventKey, schema, record, event, key, beforeKey)) {
                        inserted++;
                        // 投递任务由投递服务按自己的消费组生成（设计文档第 6.4 节）。
                    } else { duplicate++; }
                }
            }
            return new Result(inserted, duplicate, filtered, ddl);
        });
    }

    private String primaryKey(Map<String, Object> values, TableSchema schema) {
        Object value = values == null ? null : values.get(schema.primaryKey());
        if (value == null || value.toString().length() > 255) {
            throw new IllegalArgumentException("Missing or oversized primary key " + schema.table() + "." + schema.primaryKey());
        }
        return value.toString();
    }

    public static List<FieldChange> differences(BinlogEvent event) {
        return FieldChange.differences(event);
    }

    public record Result(int inserted, int duplicates, int filtered, int ddl) { }
}
