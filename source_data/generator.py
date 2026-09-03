"""
Synthetic data generator for the AI-driven production scheduling use case.

Simulates a reference factory (50 machines, 200 active orders, 10 operators/shift)
using SimPy discrete-event simulation, and streams events over two paths:

  - Machine telemetry (iot.machine.status): published over MQTT to Mosquitto,
    the way a real PLC/sensor would. Kafka Connect's MQTT source connector picks
    these up and lands them in Kafka as plain JSON.
  - Work orders, material events, operator availability (mes/erp/hr): produced
    directly to Kafka as Avro, registered against Schema Registry -- these
    stand in for systems that would realistically be database-backed in a real
    deployment (see clickhouse/setup.sql's Avro tables for the other side).

Run mode:
  - Default: fast "backfill" mode. SimPy time is decoupled from wall-clock time;
    event_time in each payload is computed from a fixed START_TIME + simulated
    offset, so you get a realistic time series fast.
  - Live demo mode: set REALTIME=True to pace the simulation at 1 sim-minute
    per REALTIME_FACTOR wall-clock seconds, so events arrive the way they would
    on a live shop floor. Good for a "watch it stream" demo.
"""

import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import numpy as np
import paho.mqtt.client as mqtt
import simpy
import simpy.rt
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from faker import Faker

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SEED = 42
random.seed(SEED)
np.random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

NUM_MACHINES = 50
NUM_ORDERS = 200
NUM_OPERATORS = 10

MACHINE_TYPES = ["CNC", "Press", "Assembly", "Paint", "Weld"]
BREAKDOWN_MTBF_HOURS = 120
BREAKDOWN_MTTR_MIN = (15, 180)
RUSH_ESCALATION_PROB_PER_CHECK = 0.02
ABSENCE_PROB_PER_SHIFT = 0.08

KAFKA_BOOTSTRAP = "10.10.20.33:7092"
SCHEMA_REGISTRY_URL = "http://10.10.20.33:7081"
MQTT_BROKER_HOST = "10.10.20.48"
MQTT_BROKER_PORT = 1883
MQTT_TOPIC_TEMPLATE = "factory/machines/{machine_id}/status"  # matches the MQTT source connector's KCQL

# Kafka topics for the three direct-producer sources (mes, erp, hr).
# iot is intentionally excluded here -- it goes over MQTT, not a direct Kafka produce.
TOPICS = {
    "mes": "mes_workorder_status",
    "erp": "erp_material_inventory",
    "hr": "hr_operator_availability",
}

SCHEMA_FILES = {
    "mes": "schemas/workorder_status.avsc",
    "erp": "schemas/material_inventory.avsc",
    "hr": "schemas/operator_availability.avsc",
}
SCHEMAS = {key: avro.load(path) for key, path in SCHEMA_FILES.items()}
KEY_SCHEMA = avro.loads('"string"')  # keys are plain strings (order_id, operator_id, etc.)

REALTIME = False          # True = pace to wall clock for a live-streaming demo
REALTIME_FACTOR = 0.05    # 1 sim-minute = 0.05 real seconds when REALTIME=True
SIM_MINUTES = 480         # one 8-hour shift
START_TIME = datetime(2026, 8, 18, 6, 0, tzinfo=timezone.utc)

# Kafka producer: mes/erp/hr only.
producer = AvroProducer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "schema.registry.url": SCHEMA_REGISTRY_URL,
        "client.id": "factory-sim",
    }
)

# MQTT client: iot only. Machines publish here; the MQTT source connector
# reads it into Kafka -- this process never touches Kafka for machine events.
_mqtt_published_count = 0
_mqtt_acked_count = 0


def _on_mqtt_publish(client, userdata, mid):
    global _mqtt_acked_count
    _mqtt_acked_count += 1


mqtt_client = mqtt.Client(client_id="factory-sim-mqtt", protocol=mqtt.MQTTv311)
mqtt_client.on_publish = _on_mqtt_publish
mqtt_client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT)
mqtt_client.loop_start()


def sim_clock(env) -> str:
    """Map SimPy's simulated minutes to an ISO timestamp for realistic backfill."""
    return (START_TIME + timedelta(minutes=env.now)).isoformat()


def emit_mqtt(env, machine_id: str, payload: dict):
    """Publish a machine telemetry event over MQTT (iot.machine.status source)."""
    global _mqtt_published_count
    payload["event_time"] = sim_clock(env)
    topic = MQTT_TOPIC_TEMPLATE.format(machine_id=machine_id)
    mqtt_client.publish(topic, json.dumps(payload), qos=1)
    _mqtt_published_count += 1


def emit_kafka(env, topic_key: str, key: str, payload: dict):
    """Produce a mes/erp/hr event directly to Kafka as Avro."""
    payload["event_time"] = sim_clock(env)
    # Avro is a fixed schema: fields not set on this event must still be present
    # as explicit None so they serialize against the schema's nullable union types.
    schema = SCHEMAS[topic_key]
    for field in schema.fields:
        payload.setdefault(field.name, None)
    producer.produce(
        topic=TOPICS[topic_key],
        value=payload,
        value_schema=schema,
        key=key,
        key_schema=KEY_SCHEMA,
        callback=_delivery_report,
    )
    producer.poll(0)


def _delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed for {msg.key()}: {err}")


# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------
@dataclass
class Machine:
    id: str
    type: str
    status: str = "idle"


@dataclass
class Order:
    id: str
    product: str
    qty: int
    due_date: str
    status: str = "queued"
    priority: str = "normal"


@dataclass
class Operator:
    id: str
    name: str
    shift: str
    available: bool = True


def build_factory():
    machines = [
        Machine(id=f"M-{i:03d}", type=random.choice(MACHINE_TYPES))
        for i in range(NUM_MACHINES)
    ]
    orders = [
        Order(
            id=f"O-{i:04d}",
            product=fake.bothify(text="SKU-####"),
            qty=random.randint(10, 500),
            due_date=(START_TIME + timedelta(days=random.randint(1, 14))).date().isoformat(),
        )
        for i in range(NUM_ORDERS)
    ]
    operators = [
        Operator(id=f"OP-{i:02d}", name=fake.name(), shift=random.choice(["A", "B", "C"]))
        for i in range(NUM_OPERATORS)
    ]
    return machines, orders, operators


# ---------------------------------------------------------------------------
# SimPy processes
# ---------------------------------------------------------------------------
def machine_process(env, machine: Machine):
    while True:
        yield env.timeout(random.expovariate(1 / 5))
        machine.status = "running"
        emit_mqtt(env, machine.id, {"machine_id": machine.id, "status": "running", "type": machine.type})

        run_time = min(
            random.expovariate(1 / (BREAKDOWN_MTBF_HOURS * 60)),
            random.uniform(20, 90),
        )
        yield env.timeout(run_time)

        if random.random() < 0.15:
            machine.status = "down"
            repair_min = random.uniform(*BREAKDOWN_MTTR_MIN)
            emit_mqtt(env, machine.id, {
                "machine_id": machine.id, "status": "down",
                "reason": random.choice(["tool_wear", "electrical_fault", "jam"]),
                "expected_repair_min": round(repair_min, 1),
            })
            yield env.timeout(repair_min)

        machine.status = "idle"
        emit_mqtt(env, machine.id, {"machine_id": machine.id, "status": "idle"})


def order_process(env, order: Order):
    while order.status != "complete":
        yield env.timeout(random.uniform(30, 240))
        if order.status == "queued" and random.random() < 0.7:
            order.status = "in_progress"
        elif order.status == "in_progress" and random.random() < 0.4:
            order.status = "complete"

        if order.priority == "normal" and random.random() < RUSH_ESCALATION_PROB_PER_CHECK:
            order.priority = "rush"
            emit_kafka(env, "erp", order.id, {"order_id": order.id, "event": "rush_escalation", "due_date": order.due_date})

        emit_kafka(env, "mes", order.id, {"order_id": order.id, "status": order.status, "priority": order.priority})


def operator_process(env, operator: Operator):
    while True:
        # Emit immediately (t=0 on first iteration), then once per subsequent shift.
        # Note: a bare `yield env.timeout(480)` as the *first* action would never fire
        # within a SIM_MINUTES=480 run, since SimPy's run(until=480) stops processing
        # events scheduled for exactly t=480 (its loop condition is strictly "< until").
        operator.available = random.random() > ABSENCE_PROB_PER_SHIFT
        emit_kafka(env, "hr", operator.id, {
            "operator_id": operator.id, "shift": operator.shift, "available": operator.available,
        })
        yield env.timeout(480)


def scripted_disruption(env, machines, orders, at_minute: int, kind: str):
    """
    Deterministic disruption injected at a known simulated time, so the demo
    reliably shows a breakdown/rush order for the rescheduling engine to react to,
    instead of relying on randomness to produce one during the walkthrough.
    """
    yield env.timeout(at_minute)
    if kind == "breakdown":
        m = random.choice(machines)
        m.status = "down"
        emit_mqtt(env, m.id, {
            "machine_id": m.id, "status": "down", "reason": "scripted_demo_breakdown",
            "expected_repair_min": 90,
        })
    elif kind == "rush_order":
        o = random.choice(orders)
        o.priority = "rush"
        emit_kafka(env, "erp", o.id, {"order_id": o.id, "event": "rush_escalation", "due_date": o.due_date})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def run():
    machines, orders, operators = build_factory()
    env = simpy.rt.RealtimeEnvironment(factor=REALTIME_FACTOR) if REALTIME else simpy.Environment()

    for m in machines:
        env.process(machine_process(env, m))
    for o in orders:
        env.process(order_process(env, o))
    for op in operators:
        env.process(operator_process(env, op))

    # scripted moments for a reliable demo of the reschedule flow
    env.process(scripted_disruption(env, machines, orders, at_minute=120, kind="breakdown"))
    env.process(scripted_disruption(env, machines, orders, at_minute=200, kind="rush_order"))

    mode = "real-time" if REALTIME else "fast backfill"
    print(f"Simulating {SIM_MINUTES} minutes ({mode} mode) -- iot over MQTT, mes/erp/hr to Kafka ...")
    env.run(until=SIM_MINUTES)
    producer.flush()

    # MQTT equivalent of producer.flush(): wait for the background network thread
    # to actually deliver every queued publish before disconnecting. Without this,
    # fast backfill mode can queue hundreds of publishes in milliseconds and then
    # disconnect before they're sent -- this was silently dropping all IoT data.
    print(f"Flushing MQTT: {_mqtt_published_count} published, waiting for acks ...")
    wait_start = time.time()
    while _mqtt_acked_count < _mqtt_published_count and (time.time() - wait_start) < 30:
        time.sleep(0.1)
    if _mqtt_acked_count < _mqtt_published_count:
        print(f"WARNING: only {_mqtt_acked_count}/{_mqtt_published_count} MQTT messages acked after 30s timeout")
    else:
        print(f"All {_mqtt_acked_count} MQTT messages acked.")

    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    print("Done.")


if __name__ == "__main__":
    run()
