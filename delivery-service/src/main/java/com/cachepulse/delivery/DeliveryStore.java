package com.cachepulse.delivery;

import com.cachepulse.common.event.RejectedEvent;
import com.cachepulse.delivery.replay.ReplayJob;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/** 投递服务自己的库 cp_delivery：订阅、投递任务、尝试记录、重放任务、源事件去重。只用显式 SQL。 */
public class DeliveryStore {
    private final JdbcTemplate jdbc;
    private final ObjectMapper json;
    private final TransactionTemplate transaction;

    public DeliveryStore(JdbcTemplate jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
        this.transaction = new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));
    }

    /**
     * 登记一条源事件，返回 false 表示已经处理过（Canal 重发同一条 binlog 时位点会变，event_id 跟着变，
     * 只能靠由 binlog 文件和位置算出的 source_event_key 去重）。调用方要和生成投递任务放在同一个本地事务里。
     */
    public boolean claimSourceEvent(String sourceEventKey, String eventId) {
        try {
            jdbc.update("INSERT INTO source_event (source_event_key,event_id) VALUES (?,?)", sourceEventKey, eventId);
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    public TransactionTemplate transaction() {
        return transaction;
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
    public String encode(Object value) {
        try { return json.writeValueAsString(value); }
        catch (JsonProcessingException e) { throw new IllegalArgumentException("Cannot encode archive data", e); }
    }

    public <T> T decode(String text, Class<T> type) {
        try { return json.readValue(text, type); }
        catch (JsonProcessingException e) { throw new IllegalStateException("Stored archive JSON is invalid", e); }
    }

    // --- Subscription Management ---

    public void saveSubscription(EventSubscription sub) {
        jdbc.update("INSERT INTO event_subscription (subscription_id,source_id,database_name,table_name,"
                        + "target_name,operations,changed_fields,filter_field,filter_value,enabled,rule_version) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                sub.subscriptionId(), sub.sourceId(), sub.database(), sub.table(),
                sub.targetName(), sub.operations(), sub.changedFields(), sub.filterField(), sub.filterValue(),
                sub.enabled(), sub.ruleVersion());
    }

    public boolean updateSubscription(EventSubscription sub) {
        int updated = jdbc.update("UPDATE event_subscription SET table_name=?,target_name=?,operations=?, changed_fields=?, filter_field=?, "
                        + "filter_value=?, enabled=?, rule_version=rule_version+1,updated_at=CURRENT_TIMESTAMP(3) WHERE subscription_id=? AND rule_version=?",
                sub.table(), sub.targetName(), sub.operations(), sub.changedFields(), sub.filterField(), sub.filterValue(), sub.enabled(),
                sub.subscriptionId(), sub.ruleVersion());
        return updated > 0;
    }

    public EventSubscription findSubscription(String id) {
        List<EventSubscription> rows = jdbc.query(
                "SELECT subscription_id,source_id,database_name,table_name,target_name,operations,changed_fields,"
                        + "filter_field,filter_value,enabled,rule_version,created_at,updated_at "
                        + "FROM event_subscription WHERE subscription_id = ?",
                (rs, idx) -> new EventSubscription(
                        rs.getString("subscription_id"),
                        rs.getString("source_id"),
                        rs.getString("database_name"),
                        rs.getString("table_name"),
                        rs.getString("target_name"),
                        rs.getString("operations"),
                        rs.getString("changed_fields"),
                        rs.getString("filter_field"),
                        rs.getString("filter_value"),
                        rs.getBoolean("enabled"),
                        rs.getInt("rule_version"),
                        rs.getTimestamp("created_at").toInstant().toString(),
                        rs.getTimestamp("updated_at").toInstant().toString()
                ), id);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public List<EventSubscription> listSubscriptions(String sourceId, String database) {
        return jdbc.query(
                "SELECT subscription_id,source_id,database_name,table_name,target_name,operations,changed_fields,"
                        + "filter_field,filter_value,enabled,rule_version,created_at,updated_at "
                        + "FROM event_subscription WHERE source_id=? AND database_name=? ORDER BY created_at DESC",
                (rs, idx) -> new EventSubscription(
                        rs.getString("subscription_id"),
                        rs.getString("source_id"),
                        rs.getString("database_name"),
                        rs.getString("table_name"),
                        rs.getString("target_name"),
                        rs.getString("operations"),
                        rs.getString("changed_fields"),
                        rs.getString("filter_field"),
                        rs.getString("filter_value"),
                        rs.getBoolean("enabled"),
                        rs.getInt("rule_version"),
                        rs.getTimestamp("created_at").toInstant().toString(),
                        rs.getTimestamp("updated_at").toInstant().toString()
                ), sourceId, database);
    }

    public List<EventSubscription> findEnabledSubscriptions(String sourceId, String database, String table) {
        return jdbc.query(
                "SELECT subscription_id,source_id,database_name,table_name,target_name,operations,changed_fields,"
                        + "filter_field,filter_value,enabled,rule_version,created_at,updated_at "
                        + "FROM event_subscription WHERE source_id=? AND database_name=? AND table_name=? AND enabled=true",
                (rs, idx) -> new EventSubscription(
                        rs.getString("subscription_id"),
                        rs.getString("source_id"),
                        rs.getString("database_name"),
                        rs.getString("table_name"),
                        rs.getString("target_name"),
                        rs.getString("operations"),
                        rs.getString("changed_fields"),
                        rs.getString("filter_field"),
                        rs.getString("filter_value"),
                        rs.getBoolean("enabled"),
                        rs.getInt("rule_version"),
                        rs.getTimestamp("created_at").toInstant().toString(),
                        rs.getTimestamp("updated_at").toInstant().toString()
                ), sourceId, database, table);
    }

    // --- Delivery Task Management ---

    public boolean createDeliveryTask(DeliveryTask task) {
        try {
            jdbc.update("INSERT INTO delivery_task (delivery_id,event_id,subscription_id,round_type,round_id,"
                            + "target_name,target_url,status,attempt_count,max_attempts,next_retry_at,"
                            + "event_payload,subscription_snapshot) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    task.deliveryId(), task.eventId(), task.subscriptionId(), task.roundType(), task.roundId(),
                    task.targetName(), task.targetUrl(), task.status(), task.attemptCount(), task.maxAttempts(),
                    new java.sql.Timestamp(java.time.Instant.parse(task.nextRetryAt()).toEpochMilli()),
                    task.eventPayload(), task.subscriptionSnapshot());
            return true;
        } catch (DuplicateKeyException e) {
            // Already created for this event+subscription+round
            return false;
        }
    }

    public DeliveryTask findDeliveryTask(long taskId) {
        List<DeliveryTask> rows = jdbc.query(
                "SELECT task_id,delivery_id,event_id,subscription_id,round_type,round_id,target_name,target_url,"
                        + "status,attempt_count,max_attempts,lease_until,lease_owner,next_retry_at,last_error,"
                        + "event_payload,subscription_snapshot,created_at,updated_at FROM delivery_task WHERE task_id=?",
                (rs, idx) -> mapDeliveryTask(rs), taskId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public List<DeliveryTask> listDeliveryTasks(String status, String subscriptionId, String eventId, int limit) {
        return listDeliveryTasks(status, subscriptionId, eventId, null, null, limit);
    }

    public List<DeliveryTask> listDeliveryTasks(String status, String subscriptionId, String eventId, String roundId, Long before, int limit) {
        StringBuilder sql = new StringBuilder("SELECT task_id,delivery_id,event_id,subscription_id,round_type,round_id,"
                + "target_name,target_url,status,attempt_count,max_attempts,lease_until,lease_owner,next_retry_at,last_error,"
                + "event_payload,subscription_snapshot,created_at,updated_at FROM delivery_task WHERE 1=1");
        List<Object> args = new java.util.ArrayList<>();
        if (status != null && !status.isBlank()) { sql.append(" AND status=?"); args.add(status); }
        if (subscriptionId != null && !subscriptionId.isBlank()) { sql.append(" AND subscription_id=?"); args.add(subscriptionId); }
        if (eventId != null && !eventId.isBlank()) { sql.append(" AND event_id=?"); args.add(eventId); }
        if (roundId != null && !roundId.isBlank()) { sql.append(" AND round_id=?"); args.add(roundId); }
        if (before != null) { sql.append(" AND task_id<?"); args.add(before); }
        sql.append(" ORDER BY task_id DESC LIMIT ?");
        args.add(limit);
        return jdbc.query(sql.toString(), (rs, idx) -> mapDeliveryTask(rs), args.toArray());
    }

    public List<DeliveryTask> claimTasks(String owner, int limit, int leaseSeconds) {
        java.sql.Timestamp now = new java.sql.Timestamp(System.currentTimeMillis());
        java.sql.Timestamp leaseUntil = new java.sql.Timestamp(System.currentTimeMillis() + leaseSeconds * 1000L);

        List<Long> taskIds = jdbc.queryForList(
                "SELECT task_id FROM delivery_task WHERE "
                        + "(status IN ('PENDING', 'RETRY_WAIT') AND next_retry_at <= ?) "
                        + "OR (status = 'IN_FLIGHT' AND lease_until < ?) "
                        + "ORDER BY task_id ASC LIMIT ?",
                Long.class, now, now, limit);

        if (taskIds.isEmpty()) return List.of();

        List<DeliveryTask> claimed = new java.util.ArrayList<>();
        for (Long id : taskIds) {
            String claimToken = owner + ":" + java.util.UUID.randomUUID();
            int updated = jdbc.update(
                    "UPDATE delivery_task SET status='IN_FLIGHT', lease_owner=?, lease_until=?, updated_at=? "
                            + "WHERE task_id=? AND ((status IN ('PENDING', 'RETRY_WAIT') AND next_retry_at <= ?) "
                            + "OR (status = 'IN_FLIGHT' AND lease_until < ?))",
                    claimToken, leaseUntil, now, id, now, now);
            if (updated > 0) {
                DeliveryTask task = findDeliveryTask(id);
                if (task != null) claimed.add(task);
            }
        }
        return claimed;
    }

    public boolean finishDelivery(DeliveryTask claimed, DeliveryAttempt attempt) {
        return transaction.execute(status -> {
            List<DeliveryTask> rows = jdbc.query("SELECT * FROM delivery_task WHERE task_id=? FOR UPDATE",
                    (rs, index) -> mapDeliveryTask(rs), claimed.taskId());
            if (rows.isEmpty()) return false;
            DeliveryTask current = rows.get(0);
            java.time.Instant now = java.time.Instant.now();
            if (!"IN_FLIGHT".equals(current.status()) || !claimed.leaseOwner().equals(current.leaseOwner())
                    || !java.time.Instant.parse(current.leaseUntil()).isAfter(now)) return false;
            saveAttempt(attempt);
            jdbc.update("UPDATE delivery_task SET status=?,attempt_count=?,next_retry_at=?,last_error=?,"
                            + "lease_owner=NULL,lease_until=NULL,updated_at=? WHERE task_id=?",
                    attempt.status(), attempt.attemptNumber(),
                    java.sql.Timestamp.from(attempt.nextRetryAt() == null ? now : java.time.Instant.parse(attempt.nextRetryAt())),
                    attempt.errorMessage(), java.sql.Timestamp.from(now), claimed.taskId());
            return true;
        });
    }

    public boolean resetTaskForRetry(long taskId) {
        java.sql.Timestamp now = new java.sql.Timestamp(System.currentTimeMillis());
        int updated = jdbc.update(
                "UPDATE delivery_task SET status='PENDING', next_retry_at=?, lease_owner=NULL, lease_until=NULL, "
                        + "max_attempts=attempt_count+1, updated_at=? WHERE task_id=? AND status='FAILED'",
                now, now, taskId);
        return updated > 0;
    }

    private void saveAttempt(DeliveryAttempt attempt) {
        java.sql.Timestamp now = new java.sql.Timestamp(System.currentTimeMillis());
        java.sql.Timestamp nextRetry = attempt.nextRetryAt() != null
                ? java.sql.Timestamp.from(java.time.Instant.parse(attempt.nextRetryAt())) : null;
        jdbc.update("INSERT INTO delivery_attempt (task_id,attempt_number,status,http_status,duration_ms,"
                        + "error_message,response_summary,next_retry_at,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                attempt.taskId(), attempt.attemptNumber(), attempt.status(), attempt.httpStatus(),
                attempt.durationMs(), attempt.errorMessage(), attempt.responseSummary(), nextRetry, now);
    }

    public List<DeliveryAttempt> listAttempts(long taskId) {
        return jdbc.query("SELECT attempt_id,task_id,attempt_number,status,http_status,duration_ms,"
                        + "error_message,response_summary,next_retry_at,created_at FROM delivery_attempt "
                        + "WHERE task_id=? ORDER BY attempt_number ASC",
                (rs, idx) -> new DeliveryAttempt(
                        rs.getLong("attempt_id"),
                        rs.getLong("task_id"),
                        rs.getInt("attempt_number"),
                        rs.getString("status"),
                        rs.getObject("http_status") != null ? rs.getInt("http_status") : null,
                        rs.getLong("duration_ms"),
                        rs.getString("error_message"),
                        rs.getString("response_summary"),
                        rs.getTimestamp("next_retry_at") != null ? rs.getTimestamp("next_retry_at").toInstant().toString() : null,
                        rs.getTimestamp("created_at").toInstant().toString()
                ), taskId);
    }

    private DeliveryTask mapDeliveryTask(ResultSet rs) throws SQLException {
        return new DeliveryTask(
                rs.getLong("task_id"),
                rs.getString("delivery_id"),
                rs.getString("event_id"),
                rs.getString("subscription_id"),
                rs.getString("round_type"),
                rs.getString("round_id"),
                rs.getString("target_name"),
                rs.getString("target_url"),
                rs.getString("status"),
                rs.getInt("attempt_count"),
                rs.getInt("max_attempts"),
                rs.getTimestamp("lease_until") != null ? rs.getTimestamp("lease_until").toInstant().toString() : null,
                rs.getString("lease_owner"),
                rs.getTimestamp("next_retry_at") != null ? rs.getTimestamp("next_retry_at").toInstant().toString() : null,
                rs.getString("last_error"),
                rs.getString("event_payload"),
                rs.getString("subscription_snapshot"),
                rs.getTimestamp("created_at").toInstant().toString(),
                rs.getTimestamp("updated_at").toInstant().toString()
        );
    }

    // --- Replay Job Management ---

    public boolean saveReplayJob(ReplayJob job) {
        try {
            jdbc.update("INSERT INTO replay_job (job_id,request_key,source_id,database_name,table_name,"
                            + "subscription_id,status,total_events,matched_events,skipped_events,failed_events,"
                            + "event_ids_json,subscription_snapshot,target_url,last_error) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    job.jobId(), job.requestKey(), job.sourceId(), job.database(), job.table(),
                    job.subscriptionId(), job.status(), job.totalEvents(), job.matchedEvents(),
                    job.skippedEvents(), job.failedEvents(), encode(job.eventIds()), job.subscriptionSnapshot(), job.targetUrl(), job.lastError());
            return true;
        } catch (DuplicateKeyException e) {
            if (findReplayJobByRequestKey(job.requestKey()) == null) throw e;
            return false;
        }
    }

    public ReplayJob findReplayJob(String jobId) {
        List<ReplayJob> rows = jdbc.query(
                "SELECT job_id,request_key,source_id,database_name,table_name,subscription_id,status,"
                        + "total_events,matched_events,skipped_events,failed_events,event_ids_json,"
                        + "subscription_snapshot,target_url,last_error,created_at,updated_at FROM replay_job WHERE job_id = ?",
                (rs, idx) -> mapReplayJob(rs), jobId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public ReplayJob findReplayJobByRequestKey(String requestKey) {
        List<ReplayJob> rows = jdbc.query(
                "SELECT job_id,request_key,source_id,database_name,table_name,subscription_id,status,"
                        + "total_events,matched_events,skipped_events,failed_events,event_ids_json,"
                        + "subscription_snapshot,target_url,last_error,created_at,updated_at FROM replay_job WHERE request_key = ?",
                (rs, idx) -> mapReplayJob(rs), requestKey);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public void updateReplayJobProgress(String jobId, String status, int matched, int skipped, int failed) {
        java.sql.Timestamp now = new java.sql.Timestamp(System.currentTimeMillis());
        jdbc.update("UPDATE replay_job SET status=?, matched_events=?, skipped_events=?, failed_events=?, last_error=NULL,updated_at=? WHERE job_id=?",
                status, matched, skipped, failed, now, jobId);
    }

    @SuppressWarnings("unchecked")
    private ReplayJob mapReplayJob(ResultSet rs) throws SQLException {
        List<String> eventIds = decode(rs.getString("event_ids_json"), List.class);
        return new ReplayJob(
                rs.getString("job_id"),
                rs.getString("request_key"),
                rs.getString("source_id"),
                rs.getString("database_name"),
                rs.getString("table_name"),
                rs.getString("subscription_id"),
                rs.getString("status"),
                rs.getInt("total_events"),
                rs.getInt("matched_events"),
                rs.getInt("skipped_events"),
                rs.getInt("failed_events"),
                eventIds,
                rs.getString("subscription_snapshot"),
                rs.getString("target_url"),
                rs.getString("last_error"),
                rs.getTimestamp("created_at").toInstant().toString(),
                rs.getTimestamp("updated_at").toInstant().toString()
        );
    }

    public ReplayJob withReplayLock(String jobId, java.util.function.Function<ReplayJob, ReplayJob> work) {
        return transaction.execute(status -> {
            List<ReplayJob> rows = jdbc.query("SELECT * FROM replay_job WHERE job_id=? FOR UPDATE", (rs, index) -> mapReplayJob(rs), jobId);
            if (rows.isEmpty()) throw new java.util.NoSuchElementException("Replay job not found");
            return work.apply(rows.get(0));
        });
    }

    public List<String> pendingReplayIds() {
        return jdbc.queryForList("SELECT job_id FROM replay_job WHERE status IN ('PENDING','RUNNING') ORDER BY created_at LIMIT 10", String.class);
    }

    public void recordReplayFailure(String jobId, String reason, boolean permanent) {
        jdbc.update("UPDATE replay_job SET status=?,last_error=?,updated_at=? WHERE job_id=? AND status IN ('PENDING','RUNNING')",
                permanent ? "FAILED" : "PENDING", reason.substring(0, Math.min(reason.length(), 1000)),
                new java.sql.Timestamp(System.currentTimeMillis()), jobId);
    }

    public java.util.Map<String, Long> replayDeliveryStats(String jobId) {
        java.util.Map<String, Long> counts = new java.util.LinkedHashMap<>();
        jdbc.query("SELECT status,COUNT(*) AS amount FROM delivery_task WHERE round_id=? GROUP BY status", rs -> {
            counts.put(rs.getString("status"), rs.getLong("amount"));
        }, jobId);
        return counts;
    }
}
