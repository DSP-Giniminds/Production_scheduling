"""
Orchestrator -- the always-on consumer that ties everything together.

DEBOUNCE: disruption events are collected into `pending` rather than
triggering an immediate reschedule each time. A reschedule fires once
COOLDOWN_SECONDS has elapsed since the last one *completed*, covering
everything that arrived in between as a single solve. This prevents the
back-to-back reschedule flood seen when many disruptions land in a short
window (e.g. fast-backfill mode), while guaranteeing nothing pending is ever
silently dropped -- the next loop iteration after cooldown expires will
always fire, even with no new incoming message, as long as something is
still pending.

Because the polling loop is single-threaded and synchronous, a reschedule in
progress blocks new message processing until it completes -- so there is no
possibility of two reschedules overlapping.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringSerializer

# Import state/solver modules -- relative to this file's location, not CWD.
_SOLVER_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "solver")
sys.path.insert(0, _SOLVER_DIR)
from state import fetch_current_machines, fetch_current_orders, fetch_reference_now  # noqa: E402
from scheduler import build_and_solve, load_cycle_time_model, select_orders  # noqa: E402

KAFKA_BOOTSTRAP = "10.10.20.33:7092"
SCHEMA_REGISTRY_URL = "http://10.10.20.33:7081"

MQTT_MACHINE_TOPIC = "mqtt"                       # iot machine status (JSON)
ERP_TOPIC = "erp_material_inventory"              # Avro
SCHEDULING_UPDATES_TOPIC = "scheduling_updates"

# Debounce: minimum real-world seconds between the END of one reschedule and
# the START of the next. Tuned low here for demo purposes against fast
# backfill data -- pick a value that makes sense for genuine real-time
# cadence in REALTIME mode / production, not just for taming this demo.
COOLDOWN_SECONDS = 15

sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
erp_deserializer = AvroDeserializer(sr_client)

_SCHEMA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schemas")
with open(os.path.join(_SCHEMA_DIR, "scheduling_updates.avsc"), "r") as f:
    output_schema_str = f.read()

value_serializer = AvroSerializer(sr_client, output_schema_str)
key_serializer = StringSerializer("utf_8")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "orchestrator",
    "auto.offset.reset": "latest",
})
consumer.subscribe([MQTT_MACHINE_TOPIC, ERP_TOPIC])

producer = SerializingProducer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "key.serializer": key_serializer,
    "value.serializer": value_serializer,
})

print("Loading cycle-time model once at startup (not per-trigger) ...")
_model, _feature_columns = load_cycle_time_model()

# Debounce state
_pending = []          # list of (reason, detail) accumulated since the last reschedule
_last_reschedule_time = 0.0


def decode_message(msg):
    """Decode MQTT JSON or ERP Avro messages."""
    if msg.topic() == MQTT_MACHINE_TOPIC:
        return json.loads(msg.value().decode("utf-8"))
    elif msg.topic() == ERP_TOPIC:
        return erp_deserializer(msg.value(), None)
    return None


def is_disruption(topic: str, payload: dict):
    """Determine if incoming message requires rescheduling."""
    if topic == MQTT_MACHINE_TOPIC and payload.get("status") == "down":
        return True, "machine_breakdown", payload.get("machine_id", "unknown")
    if topic == ERP_TOPIC and payload.get("event") == "rush_escalation":
        return True, "rush_order", payload.get("order_id", "unknown")
    return False, None, None


def publish_with_retry(payload: dict, key: str, max_attempts: int = 3) -> bool:
    """Publish with retry to handle transient Schema Registry timeouts."""
    for attempt in range(1, max_attempts + 1):
        try:
            producer.produce(topic=SCHEDULING_UPDATES_TOPIC, key=str(key), value=payload)
            return True
        except Exception as e:
            print(f"Publish attempt {attempt}/{max_attempts} failed for key '{key}': {e}")
            if attempt < max_attempts:
                time.sleep(2 ** attempt)
    print(f"Giving up publishing key '{key}' after {max_attempts} attempts.")
    return False


def run_reschedule(triggered_by: str, trigger_detail: str):
    print(f"\n=== Rescheduling (cause: {triggered_by} -- {trigger_detail}) ===")

    reference_now = fetch_reference_now()
    machines_df = fetch_current_machines()
    orders_df = fetch_current_orders(reference_now)
    orders_df = select_orders(orders_df)

    if orders_df.empty or machines_df.empty:
        print("Nothing to schedule (no orders or no machines available). Skipping.")
        return

    solver, status, assign_literals, order_end, tardiness = build_and_solve(
        machines_df, orders_df, _model, _feature_columns, reference_now
    )

    from ortools.sat.python import cp_model
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        print(f"Solver could not find a solution: {solver.StatusName(status)}")
        return

    solved_at = datetime.now(timezone.utc).isoformat()
    published_count = 0
    failed_count = 0

    for (order_id, machine_id), lit in assign_literals.items():
        if solver.Value(lit):
            payload = {
                "order_id": str(order_id),
                "machine_id": str(machine_id),
                "end_minutes": int(solver.Value(order_end[order_id])),
                "tardy_minutes": int(solver.Value(tardiness[order_id])),
                "triggered_by": str(triggered_by),
                "trigger_detail": str(trigger_detail) if trigger_detail else None,
                "solved_at": str(solved_at),
            }
            if publish_with_retry(payload, key=order_id):
                published_count += 1
            else:
                failed_count += 1

    producer.flush()
    print(
        f"Published {published_count} schedule updates to '{SCHEDULING_UPDATES_TOPIC}'"
        + (f" ({failed_count} failed after retries)." if failed_count else ".")
    )


def maybe_fire_pending():
    """
    Fire a reschedule covering everything accumulated in `_pending`, but only
    once COOLDOWN_SECONDS has elapsed since the last reschedule completed.
    Called every loop iteration so pending work is never stranded waiting for
    a new message that might not come.
    """
    global _pending, _last_reschedule_time

    if not _pending:
        return

    elapsed = time.time() - _last_reschedule_time
    if elapsed < COOLDOWN_SECONDS:
        return  # still cooling down -- leave _pending as-is, check again next loop

    reason, detail = _pending[-1]  # most recent cause as the headline reason
    if len(_pending) > 1:
        detail = f"{detail} (+{len(_pending) - 1} more disruption(s) during cooldown)"

    try:
        run_reschedule(reason, detail)
    except Exception as e:
        print(f"Reschedule cycle failed unexpectedly, but staying alive: {e}")

    _pending = []
    _last_reschedule_time = time.time()


def main():
    print(f"Orchestrator listening on '{MQTT_MACHINE_TOPIC}' and '{ERP_TOPIC}' for disruptions "
          f"(cooldown: {COOLDOWN_SECONDS}s) ...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                else:
                    try:
                        payload = decode_message(msg)
                    except Exception as e:
                        print(f"Failed to decode message on {msg.topic()}: {e}")
                        payload = None

                    if payload is not None:
                        triggered, reason, detail = is_disruption(msg.topic(), payload)
                        if triggered:
                            _pending.append((reason, detail))
                            print(f"Disruption noted: {reason} ({detail}) -- "
                                  f"{len(_pending)} pending, will fire when cooldown clears.")

            maybe_fire_pending()
    except KeyboardInterrupt:
        print("\nShutting down orchestrator ...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
