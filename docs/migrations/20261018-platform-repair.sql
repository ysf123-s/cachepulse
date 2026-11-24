-- Existing platform databases only. Stop all app instances, back up cdc_platform, and run ONCE as administrator.
-- Fresh databases already contain these columns in docker/mysql/init/03-platform.sql.
USE cdc_platform;
ALTER TABLE table_schema_snapshot ADD COLUMN recovery_restriction VARCHAR(1024);
ALTER TABLE replay_job ADD COLUMN target_url VARCHAR(512), ADD COLUMN last_error VARCHAR(1024);

CREATE TABLE recovery_baseline (
    source_id VARCHAR(64) NOT NULL,
    database_name VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    snapshot_id CHAR(64) NOT NULL,
    binlog_file VARCHAR(255) NOT NULL,
    binlog_position BIGINT NOT NULL,
    server_uuid VARCHAR(64) NOT NULL,
    verified_at TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (source_id,database_name,table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Reconcile results that the old executor saved only in delivery_attempt before interruption.
UPDATE delivery_task t
JOIN (SELECT task_id,MAX(attempt_number) AS latest FROM delivery_attempt GROUP BY task_id) a ON a.task_id=t.task_id
JOIN delivery_attempt d ON d.task_id=a.task_id AND d.attempt_number=a.latest
SET t.attempt_count=d.attempt_number,t.status=d.status,t.last_error=d.error_message,
    t.next_retry_at=COALESCE(d.next_retry_at,CURRENT_TIMESTAMP(3)),t.lease_owner=NULL,t.lease_until=NULL
WHERE t.attempt_count<d.attempt_number;

-- An unfinished old replay did not freeze its target. Preserve it as failed instead of inventing that target.
UPDATE replay_job SET status='FAILED',last_error='Legacy replay lacks frozen target; create a new replay request'
WHERE status IN ('RUNNING','PENDING');
