"""
Shared feature computation for the ML layer.

IMPORTANT: this module must be the ONLY place feature logic lives. Every
training script imports from here, and the orchestrator (built later) will
import the same functions for live inference. If feature computation is ever
duplicated instead of shared, training and inference will silently drift
apart -- the exact "train/serve skew" risk flagged earlier in this build.
"""

import clickhouse_connect
import pandas as pd

# Update these to match your actual environment.
CLICKHOUSE_HOST = "10.10.20.33"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DATABASE = "manufacturing_monitoring"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "Clickhouse@321"

# One row per completed machine "run": how long it ran (from the 'running'
# event to the next status change) and whether that run ended in a breakdown.
# This is the shared base table both cycle-time and breakdown-risk train from.
_RUN_TRANSITIONS_QUERY = """
WITH transitions AS (
    SELECT
        machine_id,
        event_time,
        status,
        lagInFrame(event_time) OVER (PARTITION BY machine_id ORDER BY event_time) AS prev_time,
        lagInFrame(status) OVER (PARTITION BY machine_id ORDER BY event_time) AS prev_status,
        -- 'type' is only ever sent on the 'running' event (see generator.py's
        -- emit_mqtt calls) -- the idle/down events that close out a run never
        -- carry it. Pull it from the preceding 'running' row instead of the
        -- current row, or every value here is NULL.
        lagInFrame(type) OVER (PARTITION BY machine_id ORDER BY event_time) AS prev_type
    FROM machine_status_history
)
SELECT
    machine_id,
    prev_type AS type,
    dateDiff('minute', prev_time, event_time) AS duration_minutes,
    if(status = 'down', 1, 0) AS ended_in_breakdown
FROM transitions
WHERE prev_status = 'running'
  AND status IN ('idle', 'down')
  AND prev_time IS NOT NULL
ORDER BY machine_id, event_time
"""


def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DATABASE,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def fetch_machine_run_transitions() -> pd.DataFrame:
    """Pull the shared base table: one row per completed machine run."""
    client = get_client()
    result = client.query(_RUN_TRANSITIONS_QUERY)
    return pd.DataFrame(result.result_rows, columns=result.column_names)


def build_cycle_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Features + label for cycle-time regression.

    Current feature set is deliberately minimal (machine type only) --
    that's genuinely all the current schema captures that's predictive of
    duration. Extend this once more signal exists (e.g. product/order type,
    if that ever gets linked to a specific machine run).
    """
    features = pd.get_dummies(df[["type"]], columns=["type"], prefix="type")
    features["duration_minutes"] = df["duration_minutes"]
    return features


def build_breakdown_risk_features(df: pd.DataFrame) -> pd.DataFrame:
    """Features + label for breakdown-risk classification."""
    features = pd.get_dummies(df[["type"]], columns=["type"], prefix="type")
    features["ended_in_breakdown"] = df["ended_in_breakdown"]
    return features


def encode_type_for_inference(machine_type: str, feature_columns: list) -> pd.DataFrame:
    """
    Build a single-row feature vector for live inference that exactly matches
    a trained model's feature_columns (same one-hot columns, same order).

    This function is the enforcement point for "training and inference must
    use identical feature logic" -- both train_cycle_time.py's training data
    and the solver's live predict calls go through get_dummies with the same
    'type_' prefix convention, so this just has to align columns, not
    re-derive the encoding logic.
    """
    row = {col: 0 for col in feature_columns}
    key = f"type_{machine_type}"
    if key in row:
        row[key] = 1
    # else: unseen machine type at inference time -- falls back to the
    # "all-zero" row, which XGBoost treats as the baseline/average case.
    return pd.DataFrame([row])
