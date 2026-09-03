-- ============================================================================
-- Manufacturing AI Platform - ClickHouse Setup
-- Kafka Broker      : 10.10.20.33:7092
-- Schema Registry   : http://10.10.20.33:7081
-- Database          : manufacturing_monitoring
-- ClickHouse        : 26.7+
-- ============================================================================

DROP DATABASE IF EXISTS manufacturing_monitoring SYNC;

CREATE DATABASE manufacturing_monitoring;
USE manufacturing_monitoring;

-- ============================================================================
-- 1. ERP Material Inventory (AvroConfluent)
-- Kafka Topic: erp_material_inventory
-- ============================================================================

CREATE TABLE erp_material_inventory_history
(
    order_id    String,
    event       String,
    due_date    Date,
    event_time  DateTime,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (order_id, event_time)
TTL event_time + INTERVAL 90 DAY;

CREATE TABLE erp_material_inventory_kafka
(
    order_id   String,
    event      String,
    due_date   String,
    event_time String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '10.10.20.33:7092',
    kafka_topic_list = 'erp_material_inventory',
    kafka_group_name = 'clickhouse-erp-material-consumer',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://10.10.20.33:7081',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_skip_broken_messages = 20;

CREATE MATERIALIZED VIEW erp_material_inventory_mv
TO erp_material_inventory_history
AS
SELECT
    order_id,
    event,
    toDate(due_date) AS due_date,
    parseDateTimeBestEffort(event_time) AS event_time,
    now() AS ingested_at
FROM erp_material_inventory_kafka;

-- ============================================================================
-- 2. MES Workorder Status (AvroConfluent)
-- Kafka Topic: mes_workorder_status
-- ============================================================================

CREATE TABLE mes_workorder_status_history
(
    order_id    String,
    status      String,
    priority    String,
    event_time  DateTime,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (order_id, event_time)
TTL event_time + INTERVAL 90 DAY;

CREATE TABLE mes_workorder_status_kafka
(
    order_id   String,
    status     String,
    priority   String,
    event_time String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '10.10.20.33:7092',
    kafka_topic_list = 'mes_workorder_status',
    kafka_group_name = 'clickhouse-mes-workorder-consumer',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://10.10.20.33:7081',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_skip_broken_messages = 20;

CREATE MATERIALIZED VIEW mes_workorder_status_mv
TO mes_workorder_status_history
AS
SELECT
    order_id,
    status,
    priority,
    parseDateTimeBestEffort(event_time) AS event_time,
    now() AS ingested_at
FROM mes_workorder_status_kafka;

-- ============================================================================
-- 3. Machine Status (JSON via MQTT Source Connector)
-- Kafka Topic: mqtt
-- CORRECTED: includes reason + expected_repair_min (needed for the breakdown-risk
-- ML model -- the original version omitted these, silently starving that model of
-- training signal). Consumer group bumped to -v2 to force a clean re-read.
-- ============================================================================

-- Target table
CREATE TABLE machine_status_history
(
    machine_id          String,
    status              String,
    type                Nullable(String),
    reason              Nullable(String),
    expected_repair_min Nullable(Float64),
    event_time          DateTime,
    ingested_at         DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (machine_id, event_time)
TTL event_time + INTERVAL 90 DAY;

-- Kafka table (JSON from MQTT connector)
CREATE TABLE machine_status_kafka
(
    machine_id          String,
    status              String,
    type                Nullable(String),
    reason              Nullable(String),
    expected_repair_min Nullable(Float64),
    event_time          String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '10.10.20.33:7092',
    kafka_topic_list = 'mqtt',
    kafka_group_name = 'clickhouse-machine-status-consumer-v2',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_skip_broken_messages = 20;

-- Materialized View
CREATE MATERIALIZED VIEW machine_status_mv
TO machine_status_history
AS
SELECT
    machine_id,
    status,
    type,
    reason,
    expected_repair_min,
    parseDateTimeBestEffort(event_time) AS event_time,
    now() AS ingested_at
FROM machine_status_kafka;

-- ============================================================================
-- 4. Operator Availability (AvroConfluent)
-- Kafka Topic: hr_operator_availability
-- ============================================================================

CREATE TABLE operator_availability_history
(
    operator_id String,
    shift       String,
    available   Bool,
    event_time  DateTime,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (operator_id, event_time)
TTL event_time + INTERVAL 90 DAY;

CREATE TABLE operator_availability_kafka
(
    operator_id String,
    shift       String,
    available   Bool,
    event_time  String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '10.10.20.33:7092',
    kafka_topic_list = 'hr_operator_availability',
    kafka_group_name = 'clickhouse-operator-availability-consumer',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://10.10.20.33:7081',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_skip_broken_messages = 20;

CREATE MATERIALIZED VIEW operator_availability_mv
TO operator_availability_history
AS
SELECT
    operator_id,
    shift,
    available,
    parseDateTimeBestEffort(event_time) AS event_time,
    now() AS ingested_at
FROM operator_availability_kafka;
