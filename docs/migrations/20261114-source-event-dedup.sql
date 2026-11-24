-- Existing platform databases: stop all archive writers and back up cdc_platform; run ONCE on MySQL 8+.
-- Keep event_id and all existing history/task/recovery records. Fresh installs use 03-platform.sql.
USE cdc_platform;
SET NAMES utf8mb4;

CREATE TEMPORARY TABLE source_event_rows_20261114 (
    sequence_id BIGINT PRIMARY KEY,
    source_row_key CHAR(64) NOT NULL,
    kafka_topic VARCHAR(249) NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    entry_index INT NOT NULL,
    row_index INT NOT NULL
);

-- Missing source positions fail this NOT NULL insert before changing the persistent schema.
INSERT INTO source_event_rows_20261114
SELECT sequence_id,
       CASE WHEN COALESCE(JSON_TYPE(JSON_EXTRACT(event_json, '$.binlogFile')), '') <> 'STRING'
                  OR JSON_UNQUOTE(JSON_EXTRACT(event_json, '$.binlogFile')) = ''
                  OR COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(event_json, '$.binlogPosition')) AS SIGNED), 0) <= 0
            THEN NULL
            ELSE SHA2(CONCAT_WS(CHAR(0), 'source-row-v1', source_id, database_name, table_name,
                 JSON_UNQUOTE(JSON_EXTRACT(event_json, '$.binlogFile')),
                 CAST(CAST(JSON_UNQUOTE(JSON_EXTRACT(event_json, '$.binlogPosition')) AS SIGNED) AS CHAR),
                 operation, primary_key_value, COALESCE(before_primary_key_value, '')), 256)
       END,
       kafka_topic, kafka_partition, kafka_offset, entry_index, row_index
FROM change_event;

CREATE TEMPORARY TABLE source_event_keys_20261114 (
    sequence_id BIGINT PRIMARY KEY,
    source_event_key CHAR(64) NOT NULL,
    INDEX idx_source_event_key (source_event_key)
);

INSERT INTO source_event_keys_20261114
SELECT sequence_id,
       SHA2(CONCAT(source_row_key, CHAR(0),
            CAST(ROW_NUMBER() OVER (
                PARTITION BY source_row_key, kafka_topic, kafka_partition, kafka_offset
                ORDER BY entry_index, row_index
            ) - 1 AS CHAR)), 256)
FROM source_event_rows_20261114;

ALTER TABLE change_event
    ADD COLUMN source_event_key CHAR(64),
    ADD UNIQUE KEY uk_source_event_key (source_event_key);

-- Each source event keeps its earliest record as the deduplication owner. Older extra copies stay NULL.
UPDATE change_event e
JOIN (SELECT source_event_key, MIN(sequence_id) AS first_sequence
      FROM source_event_keys_20261114 GROUP BY source_event_key) first_record
    ON e.sequence_id = first_record.first_sequence
SET e.source_event_key = first_record.source_event_key;

DROP TEMPORARY TABLE source_event_keys_20261114;
DROP TEMPORARY TABLE source_event_rows_20261114;
