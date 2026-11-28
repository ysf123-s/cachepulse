package com.cachepulse.archive;

import com.cachepulse.archive.recovery.RecoveryBaseline;
import com.cachepulse.archive.recovery.RecoveryPlan;
import com.cachepulse.common.event.RejectedEvent;
import com.cachepulse.common.schema.SqlGenerator;
import com.cachepulse.common.model.BinlogEvent;
import com.cachepulse.common.model.KafkaCanalRecord;
import com.cachepulse.common.platform.PlatformProperties;
import com.cachepulse.common.schema.TableSchema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.ResultSet;
import java.time.ZoneId;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Explicit SQL over the platform datasource only; no business Mapper or transaction manager is reused. */
public class ArchiveStore {
    private static final String SUMMARY_COLUMNS = "sequence_id,event_id,source_id,database_name,table_name,"
            + "operation,primary_key_name,primary_key_value,before_primary_key_value,event_time_ms,archived_at,schema_snapshot_id";
    private final JdbcTemplate jdbc;
    private final NamedParameterJdbcTemplate named;
    private final ObjectMapper json;
    private final TransactionTemplate transaction;

    public ArchiveStore(JdbcTemplate jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.named = new NamedParameterJdbcTemplate(jdbc);
        this.json = json;
        this.transaction = new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));
    }

    public void saveSchema(TableSchema schema) {
        if (jdbc.queryForObject("SELECT COUNT(*) FROM table_schema_snapshot WHERE snapshot_id=?", Long.class,
                schema.snapshotId()) > 0) {
            return;
        }
        try {
            jdbc.update("INSERT INTO table_schema_snapshot (snapshot_id,source_id,database_name,table_name,"
                            + "primary_key_name,columns_json,recovery_restriction) VALUES (?,?,?,?,?,?,?)", schema.snapshotId(), schema.sourceId(),
                    schema.database(), schema.table(), schema.primaryKey(), encode(schema.columns()), schema.recoveryRestriction());
        } catch (DuplicateKeyException e) {
            if (jdbc.queryForObject("SELECT COUNT(*) FROM table_schema_snapshot WHERE snapshot_id=?", Long.class,
                    schema.snapshotId()) != 1) {
                throw e;
            }
        }
    }

    public void invalidateSchemas(String sourceId, String database, String reason) {
        // RENAME and database-level DDL may affect more than the header's table. Recovery is disabled for the database.
        jdbc.update("UPDATE table_schema_snapshot SET invalidated_reason=? WHERE source_id=? AND database_name=?",
                reason, sourceId, database);
    }

    public void invalidateSchemasForDdl(String sourceId, String database, BinlogEvent ddl, String reason) {
        List<String> ids = jdbc.queryForList("SELECT snapshot_id FROM table_schema_snapshot WHERE source_id=? AND database_name=?",
                String.class, sourceId, database);
        for (String id : ids) {
            RecoveryBaseline baseline = baseline(findSchema(id));
            if (baseline == null || !baseline.snapshotId().equals(id) || !baseline.isBeforeBaseline(ddl)) {
                jdbc.update("UPDATE table_schema_snapshot SET invalidated_reason=? WHERE snapshot_id=?", reason, id);
            }
        }
    }

    public String invalidationReason(String snapshotId) {
        return jdbc.queryForObject("SELECT invalidated_reason FROM table_schema_snapshot WHERE snapshot_id=?",
                String.class, snapshotId);
    }

    public TableSchema findSchema(String snapshotId) {
        List<TableSchema> rows = jdbc.query("SELECT * FROM table_schema_snapshot WHERE snapshot_id=?", (rs, index) -> {
            TableSchema.ColumnDefinition[] columns = decode(rs.getString("columns_json"), TableSchema.ColumnDefinition[].class);
            return new TableSchema(rs.getString("snapshot_id"), rs.getString("source_id"), rs.getString("database_name"),
                    rs.getString("table_name"), rs.getString("primary_key_name"), List.of(columns), rs.getString("recovery_restriction"));
        }, snapshotId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public RecoveryBaseline baseline(TableSchema schema) {
        List<RecoveryBaseline> rows = jdbc.query("SELECT * FROM recovery_baseline WHERE source_id=? AND database_name=? AND table_name=?",
                (rs, index) -> new RecoveryBaseline(rs.getString("source_id"), rs.getString("database_name"), rs.getString("table_name"),
                        rs.getString("snapshot_id"), rs.getString("binlog_file"), rs.getLong("binlog_position"), rs.getString("server_uuid"),
                        rs.getTimestamp("verified_at").toInstant().toString()), schema.sourceId(), schema.database(), schema.table());
        return rows.isEmpty() ? null : rows.get(0);
    }

    public void saveBaseline(TableSchema schema, RecoveryBaseline baseline) {
        transaction.executeWithoutResult(status -> {
            saveSchema(schema);
            jdbc.update("DELETE FROM recovery_baseline WHERE source_id=? AND database_name=? AND table_name=?",
                    baseline.sourceId(), baseline.database(), baseline.table());
            jdbc.update("INSERT INTO recovery_baseline (source_id,database_name,table_name,snapshot_id,binlog_file,binlog_position,server_uuid,verified_at)"
                            + " VALUES (?,?,?,?,?,?,?,?)", baseline.sourceId(), baseline.database(), baseline.table(), baseline.snapshotId(),
                    baseline.binlogFile(), baseline.position(), baseline.serverUuid(), java.sql.Timestamp.from(java.time.Instant.parse(baseline.verifiedAt())));
            jdbc.update("UPDATE table_schema_snapshot SET invalidated_reason=NULL WHERE snapshot_id=?", schema.snapshotId());
        });
    }

    public ArchivedChange.Recovery recoveryState(TableSchema schema, BinlogEvent event, ZoneId binlogTimeZone) {
        if (schema == null) return new ArchivedChange.Recovery(false, "Schema snapshot not found");
        String invalidation = invalidationReason(schema.snapshotId());
        if (invalidation != null) return new ArchivedChange.Recovery(false, "Schema snapshot invalidated by DDL: " + invalidation);
        SqlGenerator.Eligibility eligibility = SqlGenerator.checkEligibility(schema);
        if (!eligibility.eligible()) return new ArchivedChange.Recovery(false, eligibility.reason());
        RecoveryBaseline baseline = baseline(schema);
        if (baseline == null || !baseline.snapshotId().equals(schema.snapshotId())) {
            return new ArchivedChange.Recovery(false, "Recovery baseline has not been verified for this schema");
        }
        if (event != null) {
            if (!baseline.covers(event)) return new ArchivedChange.Recovery(false, "Event precedes the verified recovery baseline");
            try { SqlGenerator.validateEvent(event, schema, binlogTimeZone); }
            catch (IllegalArgumentException | java.time.DateTimeException e) { return new ArchivedChange.Recovery(false, e.getMessage()); }
        }
        return new ArchivedChange.Recovery(true, "Eligible for a current-state pre-check; not proof of safe execution");
    }

    /**
     * 记一条处理不了的事件（设计文档 14.1 第 19 条），返回 false 表示同一位置已经记过。
     * 调用方要和本批的其他写入放在同一个本地事务里。
     */
    public boolean reject(RejectedEvent rejected) {
        try {
            jdbc.update("INSERT INTO rejected_event (rejection_key,event_id,subscription_id,kafka_topic,kafka_partition,"
                            + "kafka_offset,database_name,table_name,event_type,binlog_file,binlog_position,reason) "
                            + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rejected.rejectionKey(), rejected.eventId(),
                    rejected.subscriptionId(), rejected.kafkaTopic(), rejected.kafkaPartition(), rejected.kafkaOffset(),
                    rejected.databaseName(), rejected.tableName(), rejected.eventType(), rejected.binlogFile(),
                    rejected.binlogPosition(), rejected.reason());
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    public boolean insert(String eventId, String sourceEventKey, TableSchema schema, KafkaCanalRecord record, BinlogEvent event,
                          String key, String beforeKey) {
        try {
            jdbc.update("INSERT INTO change_event (event_id,source_event_key,source_id,database_name,table_name,operation,"
                            + "primary_key_name,primary_key_value,before_primary_key_value,event_time_ms,schema_snapshot_id,"
                            + "kafka_topic,kafka_partition,kafka_offset,entry_index,row_index,event_json) "
                            + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", eventId, sourceEventKey, schema.sourceId(), schema.database(),
                    schema.table(), event.getEventType().name(), schema.primaryKey(), key, beforeKey,
                    event.getEventTimestamp(), schema.snapshotId(), record.topic(), record.partition(), record.offset(),
                    event.getEntryIndex(), event.getRowIndex(), encode(event));
            return true;
        } catch (DuplicateKeyException e) {
            if (jdbc.queryForObject("SELECT COUNT(*) FROM change_event WHERE event_id=? OR source_event_key=?",
                    Long.class, eventId, sourceEventKey) == 0) {
                throw e;
            }
            return false;
        }
    }

    public ArchivedChange.Page search(PlatformProperties properties, ArchivedChange.Filter filter) {
        MapSqlParameterSource parameters = new MapSqlParameterSource().addValue("source", properties.getSourceId())
                .addValue("database", properties.getDatabase()).addValue("limit", filter.limit() + 1);
        StringBuilder common = new StringBuilder();
        if (filter.before() != null) { common.append(" AND sequence_id<:before"); parameters.addValue("before", filter.before()); }
        if (filter.operation() != null) { common.append(" AND operation=:operation"); parameters.addValue("operation", filter.operation()); }
        if (filter.from() != null) { common.append(" AND event_time_ms>=:from"); parameters.addValue("from", filter.from()); }
        if (filter.to() != null) { common.append(" AND event_time_ms<=:to"); parameters.addValue("to", filter.to()); }
        String sql;
        if (filter.key() == null) {
            parameters.addValue("tables", properties.getTables());
            if (filter.table() != null) { parameters.addValue("table", filter.table()); }
            sql = "SELECT " + SUMMARY_COLUMNS + " FROM change_event WHERE source_id=:source AND database_name=:database"
                    + " AND table_name IN (:tables)" + (filter.table() == null ? "" : " AND table_name=:table") + common
                    + " ORDER BY sequence_id DESC LIMIT :limit";
        } else {
            // One query with "primary_key_value=:key OR before_primary_key_value=:key" and ORDER BY sequence_id LIMIT ran
            // past the gateway's 5 s timeout on the live archive (2026-11-28): no single index serves both the OR and the
            // order. One branch per (table, key column) instead: each reads its own (source, database, table, key,
            // sequence_id) index newest first and stops at the limit; UNION drops the row that matches both columns
            // (an update that keeps the key).
            List<String> tables = filter.table() == null ? properties.getTables()
                    : properties.getTables().contains(filter.table()) ? List.of(filter.table()) : List.of();
            if (tables.isEmpty()) {
                return new ArchivedChange.Page(List.of(), null);
            }
            parameters.addValue("key", filter.key());
            List<String> branches = new ArrayList<>();
            for (int index = 0; index < tables.size(); index++) {
                parameters.addValue("table" + index, tables.get(index));
                for (String column : List.of("primary_key_value", "before_primary_key_value")) {
                    branches.add("(SELECT " + SUMMARY_COLUMNS + " FROM change_event WHERE source_id=:source AND database_name=:database"
                            + " AND table_name=:table" + index + " AND " + column + "=:key" + common
                            + " ORDER BY sequence_id DESC LIMIT :limit)");
                }
            }
            sql = String.join(" UNION ", branches) + " ORDER BY sequence_id DESC LIMIT :limit";
        }
        List<ArchivedChange.Summary> rows = named.query(sql, parameters, (rs, index) -> summary(rs));
        boolean more = rows.size() > filter.limit();
        List<ArchivedChange.Summary> items = more ? rows.subList(0, filter.limit()) : rows;
        return new ArchivedChange.Page(items, more ? items.get(items.size() - 1).sequence() : null);
    }

    public ArchivedChange find(String eventId, PlatformProperties properties) {
        List<ArchivedChange> rows = named.query("SELECT " + SUMMARY_COLUMNS + ",event_json,kafka_topic,kafka_partition,kafka_offset"
                        + " FROM change_event WHERE event_id=:id AND source_id=:source AND database_name=:database"
                        + " AND table_name IN (:tables)",
                new MapSqlParameterSource().addValue("id", eventId).addValue("source", properties.getSourceId())
                        .addValue("database", properties.getDatabase()).addValue("tables", properties.getTables()),
                (rs, index) -> {
                    BinlogEvent event = decode(rs.getString("event_json"), BinlogEvent.class);
                    return new ArchivedChange(summary(rs), event, rs.getString("kafka_topic"),
                            rs.getInt("kafka_partition"), Long.toString(rs.getLong("kafka_offset")),
                            ArchiveService.differences(event), recoveryState(findSchema(rs.getString("schema_snapshot_id")), event,
                                    ZoneId.of(properties.getBinlogTimeZone())));
                });
        return rows.isEmpty() ? null : rows.get(0);
    }

    /** Replay needs the archived payload, not field differences or recovery eligibility. */
    public BinlogEvent findEvent(String eventId, PlatformProperties properties) {
        List<BinlogEvent> rows = named.query("SELECT event_json FROM change_event WHERE event_id=:id"
                        + " AND source_id=:source AND database_name=:database AND table_name IN (:tables)",
                new MapSqlParameterSource().addValue("id", eventId).addValue("source", properties.getSourceId())
                        .addValue("database", properties.getDatabase()).addValue("tables", properties.getTables()),
                (rs, index) -> decode(rs.getString("event_json"), BinlogEvent.class));
        return rows.isEmpty() ? null : rows.get(0);
    }

    private ArchivedChange.Summary summary(ResultSet rs) throws SQLException {
        return new ArchivedChange.Summary(Long.toString(rs.getLong("sequence_id")), rs.getString("event_id"),
                rs.getString("source_id"), rs.getString("database_name"), rs.getString("table_name"),
                rs.getString("operation"), rs.getString("primary_key_name"), rs.getString("primary_key_value"),
                rs.getString("before_primary_key_value"), rs.getLong("event_time_ms"),
                rs.getTimestamp("archived_at").toInstant().toString(), rs.getString("schema_snapshot_id"));
    }

    public ObjectMapper json() {
        return json;
    }

    public String encode(Object value) {
        try { return json.writeValueAsString(value); }
        catch (JsonProcessingException e) { throw new IllegalArgumentException("Cannot encode archive data", e); }
    }

    public <T> T decode(String text, Class<T> type) {
        try { return json.readValue(text, type); }
        catch (JsonProcessingException e) { throw new IllegalStateException("Stored archive JSON is invalid", e); }
    }

    public void savePlan(RecoveryPlan plan) {
        jdbc.update("INSERT INTO recovery_plan (plan_id,event_id,source_id,database_name,table_name,"
                        + "operation,reverse_operation,primary_key_name,primary_key_value,generated_sql,"
                        + "pre_check_summary,schema_snapshot_id,executable,reason,created_at) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                plan.planId(), plan.eventId(), plan.sourceId(), plan.database(), plan.table(),
                plan.eventType(), plan.reverseType(), plan.primaryKeyColumn(), plan.primaryKeyValue(),
                plan.generatedSql(), plan.checkSummary(), plan.schemaSnapshotId(),
                plan.executable(), plan.reason(),
                java.sql.Timestamp.from(java.time.Instant.parse(plan.createdAt())));
    }

    public RecoveryPlan findPlan(String planId) {
        List<RecoveryPlan> rows = jdbc.query(
                "SELECT plan_id,event_id,source_id,database_name,table_name,operation,reverse_operation,"
                        + "primary_key_name,primary_key_value,generated_sql,pre_check_summary,schema_snapshot_id,"
                        + "executable,reason,created_at FROM recovery_plan WHERE plan_id = ?",
                (rs, idx) -> new RecoveryPlan(
                        rs.getString("plan_id"),
                        rs.getString("event_id"),
                        rs.getString("source_id"),
                        rs.getString("database_name"),
                        rs.getString("table_name"),
                        rs.getString("operation"),
                        rs.getString("reverse_operation"),
                        rs.getString("primary_key_name"),
                        rs.getString("primary_key_value"),
                        rs.getString("generated_sql"),
                        rs.getString("pre_check_summary"),
                        rs.getString("schema_snapshot_id"),
                        rs.getBoolean("executable"),
                        rs.getString("reason"),
                        rs.getTimestamp("created_at").toInstant().toString()
                ),
                planId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    // --- Subscription Management ---






    // --- Delivery Task Management ---











    // --- Replay Job Management ---









}
