package com.cachepulse.common.event;

import com.cachepulse.common.model.BinlogEvent;
import com.cachepulse.common.model.KafkaCanalRecord;

import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * 归档、投递服务处理不了的事件（设计文档 14.1 第 19 条）：同样的消息、同样的代码，重试多少次结果都一样，
 * 例如缺主键、Entry 解析不了、订阅的接收端别名已经删掉。记到本服务库的 {@code rejected_event} 表后跳过，
 * 后面的消息照常处理；数据库、Kafka 这类外部故障不走这里，仍然整批回滚重试。
 *
 * <p>和本批的其他写入在同一个本地事务里提交，整批回滚时一起撤销。{@code rejectionKey} 由 Kafka 位置、
 * Entry、行和订阅算出，同一位置重复投递时只记一条。</p>
 *
 * @param eventId        行级事件的 ID（和归档、投递用的同一个算法）；整条消息或整个 Entry 处理不了时为 null
 * @param subscriptionId 只是某个订阅投不了时为该订阅，整行处理不了时为 null
 */
public record RejectedEvent(String rejectionKey, String eventId, String subscriptionId, String kafkaTopic,
                            int kafkaPartition, long kafkaOffset, String databaseName, String tableName,
                            String eventType, String binlogFile, Long binlogPosition, String reason) {
    static final int MAX_REASON = 1000;

    /** 整条 Kafka 消息处理不了（比如不是解析好的 Canal Message）。 */
    public static RejectedEvent ofRecord(String sourceId, KafkaCanalRecord record, String reason) {
        return new RejectedEvent(key(sourceId, record, -1, -1, null), null, null, record.topic(), record.partition(),
                record.offset(), null, null, null, null, null, truncate(reason));
    }

    /** 一个 Entry 解析不了，还没有行数据；库名、表名和 binlog 位置取自 Entry 头。 */
    public static RejectedEvent ofEntry(String sourceId, KafkaCanalRecord record, int entryIndex, CanalEntry.Entry entry,
                                        Throwable error) {
        CanalEntry.Header header = entry.getHeader();
        return new RejectedEvent(key(sourceId, record, entryIndex, -1, null), null, null, record.topic(),
                record.partition(), record.offset(), header.getSchemaName(), header.getTableName(), null,
                header.getLogfileName(), header.getLogfileOffset(), reason(error));
    }

    /** 一行变更处理不了；subscriptionId 不为 null 时表示只是这个订阅投不了，同一行的其他订阅照常投递。 */
    public static RejectedEvent ofEvent(String sourceId, KafkaCanalRecord record, BinlogEvent event,
                                        String subscriptionId, String reason) {
        return new RejectedEvent(key(sourceId, record, event.getEntryIndex(), event.getRowIndex(), subscriptionId),
                EventIdentity.of(sourceId, record, event), subscriptionId, record.topic(), record.partition(),
                record.offset(), event.getDatabaseName(), event.getTableName(),
                event.getEventType() == null ? null : event.getEventType().name(), event.getBinlogFile(),
                event.getBinlogPosition(), truncate(reason));
    }

    public static String reason(Throwable error) {
        return truncate(error.getClass().getSimpleName() + ": " + error.getMessage());
    }

    private static String key(String sourceId, KafkaCanalRecord record, int entryIndex, int rowIndex, String subscriptionId) {
        return EventIdentity.hash(String.join("\u0000", "rejected-v1", sourceId, record.topic(),
                Integer.toString(record.partition()), Long.toString(record.offset()), Integer.toString(entryIndex),
                Integer.toString(rowIndex), subscriptionId == null ? "" : subscriptionId));
    }

    private static String truncate(String reason) {
        String text = reason == null ? "unknown" : reason;
        return text.length() <= MAX_REASON ? text : text.substring(0, MAX_REASON);
    }
}
