-- CachePulse 归档服务的库（设计文档第 7 节）：表结构快照、恢复基线、变更归档、恢复 SQL。
-- 由数据库管理员执行一次，可以重复执行；不改动业务库。表结构与原 cdc_platform 相同，切换时按表复制（第 11 节）。
CREATE DATABASE IF NOT EXISTS cp_archive
    DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

USE cp_archive;

CREATE TABLE IF NOT EXISTS table_schema_snapshot (
    snapshot_id CHAR(64) NOT NULL PRIMARY KEY,
    source_id VARCHAR(64) NOT NULL,
    database_name VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    primary_key_name VARCHAR(64) NOT NULL,
    columns_json LONGTEXT NOT NULL,
    recovery_restriction VARCHAR(1024),
    invalidated_reason VARCHAR(512),
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_snapshot_table (source_id, database_name, table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS recovery_baseline (
    source_id VARCHAR(64) NOT NULL,
    database_name VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    snapshot_id CHAR(64) NOT NULL,
    binlog_file VARCHAR(255) NOT NULL,
    binlog_position BIGINT NOT NULL,
    server_uuid VARCHAR(64) NOT NULL,
    verified_at TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (source_id, database_name, table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS change_event (
    sequence_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id CHAR(64) NOT NULL,
    source_event_key CHAR(64),
    source_id VARCHAR(64) NOT NULL,
    database_name VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    operation VARCHAR(16) NOT NULL,
    primary_key_name VARCHAR(64) NOT NULL,
    primary_key_value VARCHAR(255) NOT NULL,
    before_primary_key_value VARCHAR(255),
    event_time_ms BIGINT NOT NULL,
    archived_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    schema_snapshot_id CHAR(64) NOT NULL,
    kafka_topic VARCHAR(249) NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    entry_index INT NOT NULL,
    row_index INT NOT NULL,
    event_json LONGTEXT NOT NULL,
    UNIQUE KEY uk_event_id (event_id),
    UNIQUE KEY uk_source_event_key (source_event_key),
    INDEX idx_event_table_key (source_id, database_name, table_name, primary_key_value, sequence_id),
    INDEX idx_event_table_old_key (source_id, database_name, table_name, before_primary_key_value, sequence_id),
    INDEX idx_event_table_time (source_id, database_name, table_name, event_time_ms, sequence_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS recovery_plan (
    plan_id CHAR(64) NOT NULL PRIMARY KEY,
    event_id CHAR(64) NOT NULL,
    source_id VARCHAR(64) NOT NULL,
    database_name VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    operation VARCHAR(16) NOT NULL,
    reverse_operation VARCHAR(16) NOT NULL,
    primary_key_name VARCHAR(64) NOT NULL,
    primary_key_value VARCHAR(255) NOT NULL,
    generated_sql LONGTEXT NOT NULL,
    pre_check_summary LONGTEXT,
    schema_snapshot_id CHAR(64) NOT NULL,
    executable BOOLEAN NOT NULL DEFAULT FALSE,
    reason VARCHAR(512),
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_plan_event (event_id),
    INDEX idx_plan_table (source_id, database_name, table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
