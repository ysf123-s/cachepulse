package com.cachepulse.delivery;

import com.cachepulse.common.event.EventIdentity;
import com.cachepulse.common.event.RejectedEvent;
import com.cachepulse.common.kafka.BackgroundWritePacer;
import com.cachepulse.common.kafka.CanalConnectorWrapper;
import com.cachepulse.common.model.BinlogEvent;
import com.cachepulse.common.model.ConsumerError;
import com.cachepulse.common.model.KafkaCanalRecord;
import com.cachepulse.common.model.UnprocessableEventException;
import com.cachepulse.common.parser.BinlogParser;
import com.cachepulse.common.platform.PlatformProperties;
import com.cachepulse.common.schema.TableSchema;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.InterruptException;

import java.time.DateTimeException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 投递服务自己的消费组：直接消费变更流，匹配订阅后生成投递任务，不经过归档服务（设计文档第 6.4 节）。
 * 一次拉取的所有源事件登记和投递任务在同一个本地事务里提交，提交后才确认 Kafka 位点；
 * 重复投递的 binlog 由 source_event 表去重，只生成一次任务。
 */
@Slf4j
public class DeliveryConsumer implements AutoCloseable {
    private final CanalConnectorWrapper connector;
    private final BinlogParser parser;
    private final PlatformProperties properties;
    private final TableCatalog tables;
    private final DeliveryStore store;
    private final BackgroundWritePacer pacer = new BackgroundWritePacer();
    private final AtomicLong newEvents = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong tasksCreated = new AtomicLong();
    private final AtomicLong rejectedEvents = new AtomicLong();
    private final Map<String, String> acknowledgedOffsets = new ConcurrentHashMap<>();
    private volatile boolean running;
    private volatile boolean active;
    private volatile Instant lastSuccessAt;
    private volatile ConsumerError lastError;
    private Thread thread;

    public DeliveryConsumer(CanalConnectorWrapper connector, BinlogParser parser, PlatformProperties properties,
                            TableCatalog tables, DeliveryStore store) {
        this.connector = connector;
        this.parser = parser;
        this.properties = properties;
        this.tables = tables;
        this.store = store;
    }

    public void start() {
        running = true;
        thread = new Thread(this::run, "platform-delivery-consumer");
        thread.start();
    }

    private void run() {
        try (CanalConnectorWrapper.PartitionConsumer consumer = connector.createPlatformConsumer(properties.getGroupId())) {
            active = true;
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    long started = System.nanoTime();
                    long before = newEvents.get();
                    if (processBatch(consumer)) {
                        pacer.awaitAfterBatch(newEvents.get() - before, started);
                    } else { Thread.sleep(10); }
                } catch (InterruptedException | InterruptException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (RuntimeException e) {
                    recordError(e);
                    consumer.rollback();
                    try { Thread.sleep(1000); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); break; }
                }
            }
        } catch (RuntimeException e) {
            if (running) { recordError(e); }
        } finally { active = false; }
    }

    public boolean processBatch(CanalConnectorWrapper.PartitionConsumer consumer) {
        List<KafkaCanalRecord> records = consumer.getRecordsWithoutAck();
        if (records.isEmpty()) { return false; }
        Result result = handle(records);
        // 本地事务已经提交，再推进 Kafka 位点。
        consumer.ack();
        newEvents.addAndGet(result.newEvents());
        duplicates.addAndGet(result.duplicates());
        tasksCreated.addAndGet(result.tasks());
        rejectedEvents.addAndGet(result.rejected());
        for (KafkaCanalRecord record : records) {
            acknowledgedOffsets.put(record.topic() + ":" + record.partition(), Long.toString(record.offset() + 1));
        }
        lastSuccessAt = Instant.now();
        if (result.ddl() > 0) { reloadCatalog(); }
        return true;
    }

    /**
     * 和归档服务相同的事件 ID、源事件键算法，同一条 binlog 在两个服务里得到同一个 eventId。
     * 处理不了的事件记到 rejected_event 后跳过（设计文档 14.1 第 19 条）：整行处理不了（缺主键、Entry 解析不了）时
     * 不登记源事件；只是某个订阅投不了（接收端别名已删除、筛选值比较不了）时，同一行的其他订阅照常生成任务。
     * 数据库出错照旧整批回滚重试。
     */
    public Result handle(List<KafkaCanalRecord> records) {
        return store.transaction().execute(status -> {
            Map<String, List<EventSubscription>> subscriptionsByTable = new HashMap<>();
            int created = 0, duplicate = 0, tasks = 0, ddl = 0, rejected = 0;
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
                        if (event.isDdl()) {
                            if (properties.getDatabase().equals(event.getDatabaseName())) { ddl++; }
                            continue;
                        }
                        TableSchema schema = tables.get(event.getDatabaseName(), event.getTableName());
                        if (schema == null) { continue; }
                        String eventId;
                        String sourceRowKey;
                        try {
                            Map<String, Object> current = event.getAfterData() == null ? event.getBeforeData() : event.getAfterData();
                            String key = primaryKey(current, schema);
                            String beforeKey = event.getBeforeData() == null ? null : primaryKey(event.getBeforeData(), schema);
                            eventId = EventIdentity.of(properties.getSourceId(), record, event);
                            sourceRowKey = EventIdentity.sourceRowKey(properties.getSourceId(), event, key, beforeKey);
                        } catch (IllegalArgumentException e) {
                            rejected += reject(RejectedEvent.ofEvent(properties.getSourceId(), record, event, null,
                                    RejectedEvent.reason(e)));
                            continue;
                        }
                        int occurrence = sourceRowOccurrences.merge(sourceRowKey, 1, Integer::sum) - 1;
                        String sourceEventKey = EventIdentity.sourceEventKey(sourceRowKey, occurrence);
                        if (!store.claimSourceEvent(sourceEventKey, eventId)) { duplicate++; continue; }
                        created++;
                        List<EventSubscription> subscriptions = subscriptionsByTable.computeIfAbsent(schema.table(),
                                table -> store.findEnabledSubscriptions(properties.getSourceId(), schema.database(), table));
                        Created result = createTasks(record, eventId, event, subscriptions);
                        tasks += result.tasks();
                        rejected += result.rejected();
                    }
                }
            }
            return new Result(created, duplicate, tasks, ddl, rejected);
        });
    }

    private record Created(int tasks, int rejected) { }

    private Created createTasks(KafkaCanalRecord record, String eventId, BinlogEvent event,
                                List<EventSubscription> subscriptions) {
        int created = 0, rejected = 0;
        String payload = null;
        for (EventSubscription sub : subscriptions) {
            try {
                if (!SubscriptionMatcher.matches(event, sub)) { continue; }
                PlatformProperties.TargetConfig target = properties.getTargets().get(sub.targetName());
                if (target == null) {
                    // 订阅还在、接收端别名已从 Nacos 删掉：只是这个订阅投不了，恢复别名后可用重放补投
                    rejected += reject(RejectedEvent.ofEvent(properties.getSourceId(), record, event,
                            sub.subscriptionId(), "Subscription target is not registered: " + sub.targetName()));
                    continue;
                }
                if (payload == null) { payload = store.encode(event); }
                String now = Instant.now().toString();
                String deliveryId = EventIdentity.hash("delivery:" + eventId + ":" + sub.subscriptionId() + ":INITIAL");
                if (store.createDeliveryTask(new DeliveryTask(0L, deliveryId, eventId, sub.subscriptionId(), "INITIAL", "INITIAL",
                        sub.targetName(), target.getUrl(), "PENDING", 0, properties.getMaxAttempts(), null, null,
                        now, null, payload, store.encode(sub), now, now))) {
                    created++;
                }
            } catch (IllegalArgumentException | DateTimeException e) {
                // 筛选值和这一行的值比较不了（如零日期）、内容无法编码：同样的消息重试结果也一样
                rejected += reject(RejectedEvent.ofEvent(properties.getSourceId(), record, event,
                        sub.subscriptionId(), RejectedEvent.reason(e)));
            }
        }
        return new Created(created, rejected);
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
                        + "binlog={}:{} eventId={} subscription={} reason={}", rejected.kafkaTopic(), rejected.kafkaPartition(),
                rejected.kafkaOffset(), rejected.databaseName(), rejected.tableName(), rejected.binlogFile(),
                rejected.binlogPosition(), rejected.eventId(), rejected.subscriptionId(), rejected.reason());
        return store.reject(rejected) ? 1 : 0;
    }

    private void reloadCatalog() {
        try {
            tables.reload();
        } catch (Exception e) {
            log.warn("Table metadata reload after DDL failed; subscription validation keeps the previous columns", e);
        }
    }

    private void recordError(RuntimeException error) {
        lastError = new ConsumerError(Instant.now(), Thread.currentThread().getName(),
                error.getClass().getSimpleName(), error.getMessage());
        log.error("Delivery consumer failed; Kafka progress is not advanced", error);
    }

    public Status status() {
        return new Status(running, active, properties.getGroupId(), lastSuccessAt, lastError, newEvents.get(),
                duplicates.get(), tasksCreated.get(), rejectedEvents.get(), Map.copyOf(acknowledgedOffsets));
    }

    @Override
    public void close() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            try { thread.join(5000); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    /** rejected：本批新记到 rejected_event 的事件数，按行或按订阅计（同一位置重复投递时不再计）。 */
    public record Result(int newEvents, int duplicates, int tasks, int ddl, int rejected) { }

    /** acknowledgedRejectedEvents：启动以来记到 rejected_event 并跳过的事件数（设计文档 14.1 第 19 条）。 */
    public record Status(boolean serviceStarted, boolean activeConsumer, String groupId, Instant lastSuccessAt,
                         ConsumerError lastError, long acknowledgedNewRows, long acknowledgedDuplicateRows,
                         long createdTasks, long acknowledgedRejectedEvents, Map<String, String> acknowledgedNextOffsets) { }
}
