-- ============================================================================
-- scheduling_updates ingestion
-- Kafka Topic     : scheduling_updates (Avro, produced by orchestrator.py)
-- Kafka Broker    : 10.10.20.33:7092
-- Schema Registry : http://10.10.20.33:7081
-- Run inside: manufacturing_monitoring database (matches the rest of the setup)
-- ============================================================================

USE manufacturing_monitoring;

CREATE TABLE scheduling_updates_history
(
    order_id       String,
    machine_id     String,
    end_minutes    Int32,
    tardy_minutes  Int32,
    triggered_by   String,
    trigger_detail Nullable(String),
    solved_at      DateTime,
    ingested_at    DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(solved_at)
ORDER BY (order_id, solved_at)
TTL solved_at + INTERVAL 90 DAY;

CREATE TABLE scheduling_updates_kafka
(
    order_id       String,
    machine_id     String,
    end_minutes    Int32,
    tardy_minutes  Int32,
    triggered_by   String,
    trigger_detail Nullable(String),
    solved_at      String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '10.10.20.33:7092',
    kafka_topic_list = 'scheduling_updates',
    kafka_group_name = 'clickhouse-scheduling-updates-consumer',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://10.10.20.33:7081',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_skip_broken_messages = 20;

CREATE MATERIALIZED VIEW scheduling_updates_mv
TO scheduling_updates_history
AS
SELECT
    order_id,
    machine_id,
    end_minutes,
    tardy_minutes,
    triggered_by,
    trigger_detail,
    parseDateTimeBestEffort(solved_at) AS solved_at,
    now() AS ingested_at
FROM scheduling_updates_kafka;
