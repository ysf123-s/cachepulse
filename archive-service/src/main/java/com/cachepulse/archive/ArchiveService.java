package com.cachepulse.archive;

import com.cachepulse.common.event.EventIdentity;
import com.cachepulse.common.event.FieldChange;
import com.cachepulse.common.event.RejectedEvent;
import com.cachepulse.common.model.BinlogEvent;
import com.cachepulse.common.model.KafkaCanalRecord;
import com.cachepulse.common.model.UnprocessableEventException;
import com.cachepulse.common.parser.BinlogParser;
import com.cachepulse.common.platform.PlatformProperties;
import com.cachepulse.common.schema.TableSchema;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
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

    /**
     * One Kafka poll shares a local archive transaction, independent of source MySQL transaction boundaries.
     * 处理不了的事件（缺主键、Entry 解析不了等）记到 rejected_event 后跳过，不再让一条坏消息卡住整个分区；
     * 数据库出错照旧整批回滚重试（设计文档 14.1 第 19 条）。
     */
    public Result archiveBatch(List<KafkaCanalRecord> records) {
        return transaction.execute(status -> {
            int inserted = 0, duplicate = 0, filtered = 0, ddl = 0, rejected = 0;
            for (KafkaCanalRecord record : records) {
                if (record.message() == null || record.message().isRaw()) {
                    rejected += reject(RejectedEvent.ofRecord(properties.getSourceId(), record,
                            "Expected a decoded Canal Message"));
                    continue;
                }
                List<CanalEntry.Entry> entries = record.message().getEntries();
                Map<String, Integer> sourceRowOccurrences = new HashMap<>();
                for (int entryIndex = 0; entryIndex < entries.size(); entryIndex++) {
                    List<BinlogEvent> events;
                    try {
                        events = parser.parseEntry(entries.get(entryIndex), entryIndex);
                    } catch (UnprocessableEventException e) {
                        rejected += reject(RejectedEvent.ofEntry(properties.getSourceId(), record, entryIndex,
                                entries.get(entryIndex), e));
                        continue;
                    }
                    for (BinlogEvent event : events) {
                        if (event.isDdl() && properties.getDatabase().equals(event.getDatabaseName())) {
                            store.invalidateSchemasForDdl(properties.getSourceId(), properties.getDatabase(), event,
                                    event.getEventType() + " at " + record.topic() + ":" + record.partition() + ":" + record.offset());
                            ddl++;
                            continue;
                        }
                        TableSchema schema = tables.get(event.getDatabaseName(), event.getTableName());
                        if (schema == null) { filtered++; continue; }
                        try {
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
                        } catch (IllegalArgumentException e) {
                            // 缺主键、主键超长、没有 binlog 位置、内容无法编码：同样的消息重试结果也一样
                            rejected += reject(RejectedEvent.ofEvent(properties.getSourceId(), record, event, null,
                                    RejectedEvent.reason(e)));
                        }
                    }
                }
            }
            return new Result(inserted, duplicate, filtered, ddl, rejected);
        });
    }

    private String primaryKey(Map<String, Object> values, TableSchema schema) {
        Object value = values == null ? null : values.get(schema.primaryKey());
        if (value == null || value.toString().length() > 255) {
            throw new IllegalArgumentException("Missing or oversized primary key " + schema.table() + "." + schema.primaryKey());
        }
        return value.toString();
    }

    private int reject(RejectedEvent rejected) {
        log.error("Rejected unprocessable event; it is recorded in rejected_event and skipped: kafka={}:{}:{} table={}.{} "
                        + "binlog={}:{} eventId={} reason={}", rejected.kafkaTopic(), rejected.kafkaPartition(),
                rejected.kafkaOffset(), rejected.databaseName(), rejected.tableName(), rejected.binlogFile(),
                rejected.binlogPosition(), rejected.eventId(), rejected.reason());
        return store.reject(rejected) ? 1 : 0;
    }

    public static List<FieldChange> differences(BinlogEvent event) {
        return FieldChange.differences(event);
    }

    /** rejected：本批新记到 rejected_event 的事件数（同一位置重复投递时不再计）。 */
    public record Result(int inserted, int duplicates, int filtered, int ddl, int rejected) { }
}
