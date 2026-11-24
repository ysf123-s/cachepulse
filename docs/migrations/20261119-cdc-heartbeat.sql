-- CDC heartbeat table in the business database (the one Canal captures and partitions by primary key).
-- Run before enabling canal.heartbeat. Fresh installs get it from docker/mysql/init/01-schema.sql.
-- Note: this CREATE TABLE is a DDL in the archived database, so recovery baselines must be confirmed again
-- afterwards (POST /platform/tables/{table}/recovery-baseline).
USE demo_db;

CREATE TABLE IF NOT EXISTS cdc_heartbeat (
    id INT NOT NULL PRIMARY KEY,
    beat_ms BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
