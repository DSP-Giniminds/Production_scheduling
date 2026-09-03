# AI-Driven Production Scheduling — POC

Internal proof-of-concept for the AI-driven production scheduling use case:
real-time factory event ingestion, an ML + constraint-solver decision layer,
and an always-on orchestrator that reschedules automatically when a machine
breaks down or an order gets escalated — replacing the static, snapshot-based
APS pattern described in the original solution deck.

Runs on Confluent Platform (Kafka + Schema Registry + Connect) plus open
source everywhere else: Mosquitto (MQTT), ClickHouse, OR-Tools, XGBoost/
MLflow, FastAPI. Only the streaming platform is Confluent.

\---

## Architecture

```
Sources (IoT/MQTT, MES/ERP/HR direct Avro producers)
   -> Kafka Connect (MQTT source connector) + direct Kafka production
   -> Kafka topics (Schema Registry validated)
   +--> ClickHouse (native Kafka engine ingest)  --> Dashboard (FastAPI)
   +--> Orchestrator (subscribed to disruption-relevant topics)
           -> queries ClickHouse for current state
           -> calls ML models in-process (cycle time, breakdown risk)
           -> feeds OR-Tools solver -> produces the schedule
           -> publishes to scheduling\_updates topic
                 +--> ClickHouse (same ingestion pattern) --> Dashboard
                 +--> (ERP/MES write-back connector -- not yet built)
```

\---

## Directory structure

```
source\_data/        generator.py -- SimPy factory simulator. Publishes IoT
                     telemetry over MQTT (Mosquitto -> MQTT source connector
                     -> Kafka topic `mqtt`, JSON); produces MES/ERP/HR events
                     directly to Kafka as Avro. schemas/ holds those three
                     Avro schemas. iot-machine-mqtt-source.json is the Kafka
                     Connect config for the MQTT source connector.
clickhouse\_scripts/  clickhouse\_manufacturing\_setup.sql (all 4 source topics
                     -- includes the reason/expected\_repair\_min fix for
                     machine\_status\_history, merged in from what was actually
                     deployed) and scheduling\_updates\_setup.sql (orchestrator
                     output topic).
ml/                  features.py (shared ClickHouse feature queries --
                     imported by both training and the solver, so the two
                     can never compute features differently), train\_cycle\_
                     time.py, train\_breakdown\_risk.py, changeover\_cost.py
                     (heuristic, not trained -- see Known Limitations).
                     models/ holds the trained .joblib artifacts.
solver/              state.py (current factory state from ClickHouse),
                     scheduler.py (OR-Tools CP-SAT model).
orchestrator/         orchestrator.py -- the always-on consumer tying
                     everything together (detect disruption -> query state
                     -> ML -> solve -> publish), with debounce and retry
                     built in. schemas/ holds its output Avro schema.
dashboard/           FastAPI + ClickHouse dashboard (db.py, main.py,
                     templates/index.html).
requirements.txt     single consolidated dependency list for everything
                     above.
```

\---

## Environment (this deployment)

|Component|Address|
|-|-|
|Kafka broker|`10.10.20.33:7092`|
|Schema Registry|`http://10.10.20.33:7081`|
|Kafka Connect REST|`http://localhost:8083` (on `ginicoeapp02`)|
|MQTT broker (Mosquitto, Docker)|`10.10.20.48:1883`|
|ClickHouse|`10.10.20.33:8123`, user `default`|
|Dashboard|`http://<host>:8000` (uvicorn)|

This is a **shared, multi-tenant cluster** — other pipelines already run on
it (ClickHouse sinks, a Flink job, a Neo4j sink). Check topic/subject naming
with whoever else uses it before extending this further.

\---

## Setup

```bash
pip install -r requirements.txt --break-system-packages
```

## Run order (from scratch)

1. **Mosquitto** (Docker) — MQTT broker for IoT telemetry.
2. **MQTT source connector** — register
`source\_data/iot-machine-mqtt-source.json` against Kafka Connect so
`mqtt`-topic messages land in Kafka.
3. **`source\_data/generator.py`** — run once to populate all four topics
(`mqtt`, `mes\_workorder\_status`, `erp\_material\_inventory`,
`hr\_operator\_availability`) with one simulated 8-hour shift.
4. **ClickHouse setup** — run `clickhouse\_scripts/clickhouse\_manufacturing\_ setup.sql`, then `clickhouse\_scripts/scheduling\_updates\_setup.sql`,
against the `manufacturing\_monitoring` database.
5. **Train the ML models** — `cd ml/ \&\& python train\_cycle\_time.py \&\& python train\_breakdown\_risk.py`. Saves to `ml/models/\*.joblib`.
6. **Validate the solver standalone** — `cd solver/ \&\& python scheduler.py`.
Should print `OPTIMAL` with an assignment table.
7. **Start the orchestrator** — `cd orchestrator/ \&\& python orchestrator.py`.
Leave running; it's the always-on piece.
8. **Trigger a disruption** — rerun `generator.py` in a separate terminal.
Watch the orchestrator's terminal for `=== Rescheduling ===`.
9. **Start the dashboard** — `cd dashboard/ \&\& uvicorn main:app --host 0.0.0.0 --port 8000`. Open in a browser.

\---

## Known limitations (deliberate, documented in-code)

Real gaps in the current data/schema, not bugs — each is flagged with a
comment at its source:

* **No real `due\_date` on most orders.** `mes.workorder.status` never
carries one; only orders with a rush-escalation event on the ERP topic do.
Others fall back to a placeholder (`solver/state.py`,
`DEFAULT\_DUE\_DATE\_HOURS\_AHEAD`). Real fix: add `due\_date` to the MES
payload/schema.
* **No order-to-machine compatibility constraint.** The solver treats every
available machine as a valid candidate for every order.
* **Changeover cost is a flat heuristic, not trained** (`ml/changeover\_ cost.py`) — the schema has no field linking an order to the machine it ran
on, so there's no historical changeover to learn from.
* **Breakdown-risk model AUC \~0.5 (no better than random)** — accurate, not
broken: `generator.py`'s breakdown probability is a flat 15% per machine,
independent of type, so there's genuinely nothing for the model to find.
* **Order count capped at `MAX\_ORDERS` (30)** in `scheduler.py` — a stand-in
for real scoped/pinned rescheduling, not yet implemented.
* **`generator.py` has a fixed seed and `START\_TIME`** — reruns produce
byte-identical data. Fine for one clean run; needs fixing before using it
to accumulate more training volume.
* **Debounce cooldown (`orchestrator.py`, `COOLDOWN\_SECONDS = 15`)** is tuned
for demo visibility against fast-backfill data, not real-time cadence.

## 

