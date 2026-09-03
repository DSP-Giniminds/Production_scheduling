"""
ClickHouse query layer for the dashboard. Same connection pattern as
solver/state.py and ml/features.py -- kept self-contained here so the
dashboard has no dependency on the solver/ml directories, just ClickHouse.
"""

import clickhouse_connect

CLICKHOUSE_HOST = "10.10.20.33"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DATABASE = "manufacturing_monitoring"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "Clickhouse@321"


def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DATABASE,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def _rows_as_dicts(result):
    cols = result.column_names
    return [dict(zip(cols, row)) for row in result.result_rows]


def machine_status_summary():
    client = get_client()
    query = """
    SELECT status, count() AS cnt
    FROM (
        SELECT machine_id, argMax(status, event_time) AS status
        FROM machine_status_history
        GROUP BY machine_id
    )
    GROUP BY status
    ORDER BY status
    """
    return _rows_as_dicts(client.query(query))


def machine_status_list():
    client = get_client()
    query = """
    SELECT machine_id, argMax(status, event_time) AS status, argMax(type, event_time) AS type
    FROM machine_status_history
    GROUP BY machine_id
    ORDER BY machine_id
    """
    return _rows_as_dicts(client.query(query))


def order_status_summary():
    client = get_client()
    query = """
    SELECT status, count() AS cnt
    FROM (
        SELECT order_id, argMax(status, event_time) AS status
        FROM mes_workorder_status_history
        GROUP BY order_id
    )
    GROUP BY status
    ORDER BY status
    """
    return _rows_as_dicts(client.query(query))


def recent_reschedules(limit: int = 25):
    client = get_client()
    query = f"""
    SELECT order_id, machine_id, end_minutes, tardy_minutes, triggered_by, trigger_detail, solved_at
    FROM scheduling_updates_history
    ORDER BY solved_at DESC
    LIMIT {int(limit)}
    """
    return _rows_as_dicts(client.query(query))


def tardiness_trend():
    client = get_client()
    query = """
    SELECT solved_at, sum(tardy_minutes) AS total_tardy, count() AS orders_in_batch
    FROM scheduling_updates_history
    GROUP BY solved_at
    ORDER BY solved_at ASC
    """
    return _rows_as_dicts(client.query(query))
