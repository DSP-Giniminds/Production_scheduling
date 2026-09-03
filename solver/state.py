"""
Pulls current factory state from ClickHouse for the solver.

NOTE ON "CURRENT STATE": this deployed schema only has *_history event-log
tables, no dedicated ReplacingMergeTree "current" tables. So "current state"
here means "the latest known row per entity," derived via argMax over full
history. Fine at this data volume; if history grows large, a materialized
current-state table would be more efficient than re-scanning on every solve.
"""

import clickhouse_connect
import pandas as pd

CLICKHOUSE_HOST = "10.10.20.33"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DATABASE = "manufacturing_monitoring"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "Clickhouse@321"

# KNOWN DATA GAP: mes.workorder.status never carries a due_date (see
# generator.py / schemas/workorder_status.avsc) -- a real due date only
# exists for orders that had a rush-escalation event on the ERP topic. Every
# other order falls back to this. The real fix is adding due_date to the
# mes.workorder.status payload so every order carries a genuine deadline.
DEFAULT_DUE_DATE_HOURS_AHEAD = 24

_CURRENT_MACHINES_QUERY = """
SELECT machine_id, argMax(status, event_time) AS status, argMax(type, event_time) AS type
FROM machine_status_history
GROUP BY machine_id
"""

_CURRENT_ORDERS_QUERY = """
SELECT order_id, argMax(status, event_time) AS status, argMax(priority, event_time) AS priority
FROM mes_workorder_status_history
GROUP BY order_id
"""

_ORDER_DUE_DATES_QUERY = """
SELECT order_id, argMax(due_date, event_time) AS due_date
FROM erp_material_inventory_history
GROUP BY order_id
"""

_LATEST_EVENT_TIME_QUERY = """
SELECT max(event_time) AS latest
FROM (
    SELECT event_time FROM machine_status_history
    UNION ALL
    SELECT event_time FROM mes_workorder_status_history
    UNION ALL
    SELECT event_time FROM erp_material_inventory_history
)
"""


def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DATABASE,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def fetch_reference_now() -> pd.Timestamp:
    """
    'Now' for a synthetic backfill dataset should be the latest timestamp
    actually present in the data, not real wall-clock time -- generator.py's
    simulated events live on their own timeline (START_TIME), which is
    already in the past relative to whenever this solver actually runs. Using
    real time as 'now' would make every due date instantly and meaninglessly
    overdue, regardless of how good the schedule actually is.
    """
    client = get_client()
    result = client.query(_LATEST_EVENT_TIME_QUERY)
    latest = result.result_rows[0][0]
    ts = pd.Timestamp(latest)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _run_query(client, query) -> pd.DataFrame:
    result = client.query(query)
    return pd.DataFrame(result.result_rows, columns=result.column_names)


def fetch_current_machines() -> pd.DataFrame:
    """All machines with their latest known status and type."""
    client = get_client()
    return _run_query(client, _CURRENT_MACHINES_QUERY)


def fetch_current_orders(reference_now: pd.Timestamp) -> pd.DataFrame:
    """
    Orders currently queued or in_progress, with priority and a due date.
    `has_real_due_date` tells the caller which rows are a genuine deadline
    versus the fallback -- worth distinguishing when interpreting results.

    reference_now should come from fetch_reference_now(), not real wall-clock
    time -- see that function's docstring for why.
    """
    client = get_client()
    orders = _run_query(client, _CURRENT_ORDERS_QUERY)
    due_dates = _run_query(client, _ORDER_DUE_DATES_QUERY)

    orders = orders[orders["status"].isin(["queued", "in_progress"])].copy()
    merged = orders.merge(due_dates, on="order_id", how="left")

    merged["due_date"] = pd.to_datetime(merged["due_date"], errors="coerce", utc=True)
    merged["has_real_due_date"] = merged["due_date"].notna()

    fallback = reference_now + pd.Timedelta(hours=DEFAULT_DUE_DATE_HOURS_AHEAD)
    merged["due_date"] = merged["due_date"].fillna(fallback)
    return merged
