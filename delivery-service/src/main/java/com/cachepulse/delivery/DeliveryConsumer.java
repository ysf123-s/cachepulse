package com.cachepulse.delivery;

import com.cachepulse.common.event.EventIdentity;
import com.cachepulse.common.kafka.BackgroundWritePacer;
import com.cachepulse.common.kafka.CanalConnectorWrapper;
import com.cachepulse.common.model.BinlogEvent;
import com.cachepulse.common.model.ConsumerError;
import com.cachepulse.common.model.KafkaCanalRecord;
import com.cachepulse.common.parser.BinlogParser;
import com.cachepulse.common.platform.PlatformProperties;
import com.cachepulse.common.schema.TableSchema;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.InterruptException;

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
        for (KafkaCanalRecord record : records) {
            acknowledgedOffsets.put(record.topic() + ":" + record.partition(), Long.toString(record.offset() + 1));
        }
        lastSuccessAt = Instant.now();
        if (result.ddl() > 0) { reloadCatalog(); }
        return true;
    }

    /** 和归档服务相同的事件 ID、源事件键算法，同一条 binlog 在两个服务里得到同一个 eventId。 */
    public Result handle(List<KafkaCanalRecord> records) {
        return store.transaction().execute(status -> {
            Map<String, List<EventSubscription>> subscriptionsByTable = new HashMap<>();
            int created = 0, duplicate = 0, tasks = 0, ddl = 0;
            for (KafkaCanalRecord record : records) {
                if (record.message() == null || record.message().isRaw()) {
                    throw new IllegalArgumentException("Expected a decoded Canal Message at " + record.topic() + ":"
                            + record.partition() + ":" + record.offset());
                }
                List<BinlogEvent> events = parser.parseIncludingDdl(record.message().getEntries());
                Map<String, Integer> sourceRowOccurrences = new HashMap<>();
                for (BinlogEvent event : events) {
                    if (event.isDdl()) {
                        if (properties.getDatabase().equals(event.getDatabaseName())) { ddl++; }
                        continue;
                    }
                    TableSchema schema = tables.get(event.getDatabaseName(), event.getTableName());
                    if (schema == null) { continue; }
                    Map<String, Object> current = event.getAfterData() == null ? event.getBeforeData() : event.getAfterData();
                    String key = primaryKey(current, schema);
                    String beforeKey = event.getBeforeData() == null ? null : primaryKey(event.getBeforeData(), schema);
                    String eventId = EventIdentity.of(properties.getSourceId(), record, event);
                    String sourceRowKey = EventIdentity.sourceRowKey(properties.getSourceId(), event, key, beforeKey);
                    int occurrence = sourceRowOccurrences.merge(sourceRowKey, 1, Integer::sum) - 1;
                    String sourceEventKey = EventIdentity.sourceEventKey(sourceRowKey, occurrence);
                    if (!store.claimSourceEvent(sourceEventKey, eventId)) { duplicate++; continue; }
                    created++;
                    List<EventSubscription> subscriptions = subscriptionsByTable.computeIfAbsent(schema.table(),
                            table -> store.findEnabledSubscriptions(properties.getSourceId(), schema.database(), table));
                    tasks += createTasks(eventId, event, subscriptions);
                }
            }
            return new Result(created, duplicate, tasks, ddl);
        });
    }

    private int createTasks(String eventId, BinlogEvent event, List<EventSubscription> subscriptions) {
        int created = 0;
        String payload = null;
        for (EventSubscription sub : subscriptions) {
            if (!SubscriptionMatcher.matches(event, sub)) { continue; }
            PlatformProperties.TargetConfig target = properties.getTargets().get(sub.targetName());
            if (target == null) {
                throw new IllegalStateException("Subscription target is not registered: " + sub.targetName());
            }
            if (payload == null) { payload = store.encode(event); }
            String now = Instant.now().toString();
            String deliveryId = EventIdentity.hash("delivery:" + eventId + ":" + sub.subscriptionId() + ":INITIAL");
            if (store.createDeliveryTask(new DeliveryTask(0L, deliveryId, eventId, sub.subscriptionId(), "INITIAL", "INITIAL",
                    sub.targetName(), target.getUrl(), "PENDING", 0, properties.getMaxAttempts(), null, null,
                    now, null, payload, store.encode(sub), now, now))) {
                created++;
            }
        }
        return created;
    }

    private String primaryKey(Map<String, Object> values, TableSchema schema) {
        Object value = values == null ? null : values.get(schema.primaryKey());
        if (value == null || value.toString().length() > 255) {
            throw new IllegalArgumentException("Missing or oversized primary key " + schema.table() + "." + schema.primaryKey());
        }
        return value.toString();
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
                duplicates.get(), tasksCreated.get(), Map.copyOf(acknowledgedOffsets));
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

    public record Result(int newEvents, int duplicates, int tasks, int ddl) { }

    public record Status(boolean serviceStarted, boolean activeConsumer, String groupId, Instant lastSuccessAt,
                         ConsumerError lastError, long acknowledgedNewRows, long acknowledgedDuplicateRows,
                         long createdTasks, Map<String, String> acknowledgedNextOffsets) { }
}
