-- CachePulse 投递服务的库（设计文档第 7 节）：订阅、投递任务、尝试记录、重放任务、源事件去重。
-- 由数据库管理员执行一次，可以重复执行；不改动业务库。前五张表与原 cdc_platform 相同，切换时按表复制（第 11 节）。
CREATE DATABASE IF NOT EXISTS cp_delivery
    DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

USE cp_delivery;

CREATE TABLE IF NOT EXISTS event_subscription (
    subscription_id CHAR(64) NOT NULL PRIMARY KEY,
    source_id VARCHAR(64) NOT NULL,
    database_name VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    target_name VARCHAR(64) NOT NULL,
    operations VARCHAR(64) NOT NULL,
    changed_fields VARCHAR(512),
    filter_field VARCHAR(64),
    filter_value VARCHAR(255),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    rule_version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_sub_table (source_id, database_name, table_name, enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS delivery_task (
    task_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    delivery_id CHAR(64) NOT NULL,
    event_id CHAR(64) NOT NULL,
    subscription_id CHAR(64) NOT NULL,
    round_type VARCHAR(16) NOT NULL DEFAULT 'INITIAL',
    round_id VARCHAR(64) NOT NULL DEFAULT 'INITIAL',
    target_name VARCHAR(64) NOT NULL,
    target_url VARCHAR(512) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    attempt_count INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    lease_until TIMESTAMP(3) NULL,
    lease_owner VARCHAR(64) NULL,
    next_retry_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    last_error VARCHAR(1024),
    event_payload LONGTEXT NOT NULL,
    subscription_snapshot LONGTEXT NOT NULL,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_event_sub_round (event_id, subscription_id, round_id),
    INDEX idx_task_claim (status, next_retry_at),
    INDEX idx_task_event (event_id),
    INDEX idx_task_sub (subscription_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS delivery_attempt (
    attempt_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    task_id BIGINT NOT NULL,
    attempt_number INT NOT NULL,
    status VARCHAR(16) NOT NULL,
    http_status INT,
    duration_ms BIGINT NOT NULL,
    error_message VARCHAR(1024),
    response_summary VARCHAR(1024),
    next_retry_at TIMESTAMP(3) NULL,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_task_attempt (task_id, attempt_number),
    INDEX idx_attempt_task (task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS replay_job (
    job_id CHAR(64) NOT NULL PRIMARY KEY,
    request_key VARCHAR(128) NOT NULL,
    source_id VARCHAR(64) NOT NULL,
    database_name VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    subscription_id CHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    total_events INT NOT NULL DEFAULT 0,
    matched_events INT NOT NULL DEFAULT 0,
    skipped_events INT NOT NULL DEFAULT 0,
    failed_events INT NOT NULL DEFAULT 0,
    event_ids_json LONGTEXT NOT NULL,
    subscription_snapshot LONGTEXT NOT NULL,
    target_url VARCHAR(512),
    last_error VARCHAR(1024),
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_replay_request (request_key),
    INDEX idx_replay_sub (subscription_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- 投递服务自己消费变更流，按源事件键去重（第 6.4 节）：和投递任务在同一个本地事务里写入。
-- 切换时从空表开始，CachePulse 从原项目停下的位点接着消费。
CREATE TABLE IF NOT EXISTS source_event (
    source_event_key CHAR(64) NOT NULL PRIMARY KEY,
    event_id CHAR(64) NOT NULL,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_source_event_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- 处理不了的事件（设计文档 14.1 第 19 条）：缺主键、Entry 解析不了、订阅的接收端别名已删除等，重试多少次结果都一样。
-- 记下后跳过，后面的消息照常处理；和本批的其他写入在同一个本地事务里提交。subscription_id 不为空时只是这个订阅没投，
-- 恢复接收端后可以用重放补投这些 event_id。
CREATE TABLE IF NOT EXISTS rejected_event (
    rejection_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    rejection_key CHAR(64) NOT NULL,
    event_id CHAR(64),
    subscription_id CHAR(64),
    kafka_topic VARCHAR(255) NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    database_name VARCHAR(64),
    table_name VARCHAR(64),
    event_type VARCHAR(16),
    binlog_file VARCHAR(255),
    binlog_position BIGINT,
    reason VARCHAR(1024) NOT NULL,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_rejection_key (rejection_key),
    INDEX idx_rejected_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
