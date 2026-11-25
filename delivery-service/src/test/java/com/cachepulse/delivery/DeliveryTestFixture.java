package com.cachepulse.delivery;

import com.cachepulse.common.event.EventIdentity;
import com.cachepulse.common.kafka.CanalConnectorWrapper;
import com.cachepulse.common.model.BinlogEvent;
import com.cachepulse.common.model.KafkaCanalRecord;
import com.cachepulse.common.parser.BinlogParser;
import com.cachepulse.common.platform.PlatformProperties;
import com.cachepulse.delivery.replay.ArchiveClient;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.h2.jdbcx.JdbcDataSource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ScriptUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.mock;

/**
 * Delivery-only fixture: H2 (MySQL mode) business database with an inventory table, delivery storage created from
 * docker/mysql/init/06-cp-delivery.sql, and the delivery consumer fed with Canal records directly (design 6.4).
 * {@link #archive} stands in for the archive service: every event the consumer accepted is "archived" there,
 * keyed by the same event ID both services compute. This does not certify MySQL compatibility.
 */
public class DeliveryTestFixture {
    private long nextBinlogPosition = 500;
    public final JdbcDataSource business = database("business");
    public final JdbcDataSource platform = database("delivery");
    public final JdbcTemplate jdbc = new JdbcTemplate(platform);
    public final ObjectMapper json = new ObjectMapper();
    public final PlatformProperties properties = new PlatformProperties();
    public final DeliveryStore store = new DeliveryStore(jdbc, json);
    public final TableCatalog tables;
    public final DeliveryConsumer consumer;
    public final Map<String, BinlogEvent> archived = new ConcurrentHashMap<>();
    public final ArchiveClient archive = eventId -> Optional.ofNullable(archived.get(eventId));

    public DeliveryTestFixture() throws Exception {
        new JdbcTemplate(business).execute("CREATE TABLE inventory (id BIGINT PRIMARY KEY, name VARCHAR(64), available INT, remark VARCHAR(255))");
        initializeDelivery(platform);
        try (Connection connection = business.getConnection()) { properties.setDatabase(connection.getCatalog()); }
        properties.setTables(List.of("inventory"));
        properties.setGroupId("cachepulse-delivery");
        tables = TableCatalog.load(properties, business, json);
        consumer = new DeliveryConsumer(mock(CanalConnectorWrapper.class), new BinlogParser(), properties, tables, store);
    }

    public static JdbcDataSource database(String prefix) {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + prefix + "_" + UUID.randomUUID().toString().replace("-", "")
                + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        return dataSource;
    }

    public static void initializeDelivery(javax.sql.DataSource target) throws Exception {
        String sql = Files.readString(Path.of("docker/mysql/init/06-cp-delivery.sql"), StandardCharsets.UTF_8);
        // Skip CREATE DATABASE / USE: H2 runs the tables in the in-memory database itself.
        sql = sql.substring(sql.indexOf("CREATE TABLE"))
                .replace(" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin", "");
        try (Connection connection = target.getConnection()) {
            ScriptUtils.executeSqlScript(connection, new ByteArrayResource(sql.getBytes(StandardCharsets.UTF_8)));
        }
    }

    /** Consumes one Kafka record the way the delivery consumer does, and archives its events for replay lookups. */
    public DeliveryConsumer.Result publish(KafkaCanalRecord record) {
        DeliveryConsumer.Result result = consumer.handle(List.of(record));
        for (BinlogEvent event : new BinlogParser().parse(record.message().getEntries())) {
            if (tables.get(event.getDatabaseName(), event.getTableName()) != null) {
                archived.putIfAbsent(EventIdentity.of(properties.getSourceId(), record, event), event);
            }
        }
        return result;
    }

    /** Event IDs (same algorithm as the archive service) of the registered-table rows in a record. */
    public List<String> eventIds(KafkaCanalRecord record) {
        return new BinlogParser().parse(record.message().getEntries()).stream()
                .filter(event -> tables.get(event.getDatabaseName(), event.getTableName()) != null)
                .map(event -> EventIdentity.of(properties.getSourceId(), record, event)).toList();
    }

    public void target(String name, String url) {
        PlatformProperties.TargetConfig target = new PlatformProperties.TargetConfig();
        target.setUrl(url);
        properties.getTargets().put(name, target);
    }

    public EventSubscription subscribe(String id, String target, String operations, boolean enabled) {
        EventSubscription sub = new EventSubscription(id, properties.getSourceId(), properties.getDatabase(),
                "inventory", target, operations, null, null, null, enabled, 1, null, null);
        store.saveSubscription(sub);
        return sub;
    }

    public KafkaCanalRecord record(long offset, CanalEntry.Entry... entries) {
        return new KafkaCanalRecord("example", 2, offset, new Message(99L, List.of(entries)));
    }

    public CanalEntry.Entry rows(CanalEntry.EventType operation, CanalEntry.RowData... rows) {
        return entry("inventory", CanalEntry.RowChange.newBuilder().setEventType(operation).addAllRowDatas(List.of(rows)).build());
    }

    public CanalEntry.Entry entry(String table, CanalEntry.RowChange change) {
        long position = nextBinlogPosition;
        nextBinlogPosition += 100;
        return CanalEntry.Entry.newBuilder().setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(CanalEntry.Header.newBuilder().setSchemaName(properties.getDatabase()).setTableName(table)
                        .setExecuteTime(1700000000000L).setLogfileName("mysql-bin.000001").setLogfileOffset(position))
                .setStoreValue(change.toByteString()).build();
    }

    public static CanalEntry.RowData inserted(String id, String name) {
        return CanalEntry.RowData.newBuilder().addAfterColumns(column("id", id, 0, Types.BIGINT, "bigint", true))
                .addAfterColumns(column("name", name, 1, Types.VARCHAR, "varchar(64)", false)).build();
    }

    public static CanalEntry.Column column(String name, String value, int index, int type, String mysqlType, boolean key) {
        CanalEntry.Column.Builder column = CanalEntry.Column.newBuilder().setName(name).setIndex(index).setSqlType(type)
                .setMysqlType(mysqlType).setIsKey(key).setIsNull(value == null).setUpdated(true);
        if (value != null) { column.setValue(value); }
        return column.build();
    }

    public long taskCount() { return jdbc.queryForObject("SELECT COUNT(*) FROM delivery_task", Long.class); }

    public long sourceEventCount() { return jdbc.queryForObject("SELECT COUNT(*) FROM source_event", Long.class); }
}
