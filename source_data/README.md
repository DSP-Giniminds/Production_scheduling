# Synthetic Factory Data POC

Fabricates realistic factory event streams and pushes them through two source
paths into Kafka, then into ClickHouse, so you can build and demo the AI
scheduling use case without real plant data.

**Source paths:**
- **IoT (machine telemetry)** → published over **MQTT** → Kafka Connect's MQTT
  source connector → `iot.machine.status` (plain JSON)
- **MES / ERP / HR** (work orders, material events, operator availability) →
  produced **directly to Kafka** as Avro from `generator.py` → their respective
  topics, schema-validated via Schema Registry

## 1. Start the stack

```bash
docker compose up -d
```

Brings up Confluent Platform (Kafka broker + Schema Registry + Connect —
Connect now only hosts the MQTT source connector), Mosquitto (open-source MQTT
broker), ClickHouse, and Grafana.

Kafka: `localhost:9092` · Schema Registry: `localhost:8081` · Connect REST:
`localhost:8083` · MQTT: `localhost:1883` · ClickHouse HTTP: `localhost:8123` ·
Grafana: `localhost:3000`

## 2. Install the MQTT source connector plugin (one-time)

```bash
docker exec connect confluent-hub install --no-prompt lensesio/stream-reactor-mqtt:latest
docker restart connect
```

Then register it:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @connectors/iot-machine-mqtt-source.json \
  http://localhost:8083/connectors
```

This subscribes to `factory/machines/+/status` on Mosquitto and lands each
message into the `iot.machine.status` Kafka topic as JSON.

## 3. Install Python dependencies

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

## 4. Run the generator

```bash
python generator.py
```

Machine events publish to MQTT (`factory/machines/<id>/status`); work orders,
material events, and operator availability produce directly to Kafka as Avro,
registering schemas from `schemas/*.avsc` on first run.

Check the MQTT side is flowing: `mosquitto_sub -h localhost -t 'factory/machines/#'`
Check Avro schemas registered: `curl http://localhost:8081/subjects`

By default this runs in **fast backfill mode** — a full 8-hour shift in seconds,
with event timestamps back-dated from `START_TIME`. Set `REALTIME = True` in
`generator.py` for a paced live-streaming demo instead.

## 5. Load data into ClickHouse

```bash
docker exec -i clickhouse clickhouse-client --multiquery < clickhouse/setup.sql
```

Creates, per topic: a `Kafka` engine table (native consumer — `iot.machine.status`
reads `JSONEachRow`, the other three read `AvroConfluent`), a `MergeTree` history
table, and — for machines, orders, and operators — a `ReplacingMergeTree`
current-state table queried with `FINAL`.

Adjust `kafka_broker_list` / `kafka_schema_registry_url` in `setup.sql` to match
your actual environment if it differs from `localhost:9092` / `localhost:8081`.

Verify:
```sql
SELECT count() FROM factory.iot_machine_status_history;
SELECT * FROM factory.iot_machine_status_current FINAL LIMIT 5;
```

See `clickhouse/example_queries.sql` for the queries the solver/orchestrator
and ML layers run against this data.

## 6. Point Grafana at ClickHouse

Add ClickHouse as a Grafana data source (official ClickHouse plugin or Altinity
plugin) at `http://clickhouse:8123`, then build panels against the `*_history`
and `*_current` tables.

## Repeatability

Fixed random seed (`SEED = 42`) plus two scripted disruptions (a breakdown at
sim-minute 120, a rush order at sim-minute 200) — every run produces the same
demo-able sequence. Reset consumer group offsets between runs for a clean replay.

## Retired from this pipeline

`connectors/timescaledb-sink.json` and `redis_current_state.py` are earlier
design iterations (TimescaleDB + Redis, then Kafka Connect JDBC sink) — kept
for reference only, not part of the active pipeline. `schemas/machine_status.avsc`
is similarly unused now that IoT events travel as plain JSON over MQTT rather
than Avro.
