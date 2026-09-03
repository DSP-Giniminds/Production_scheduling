"""
Production scheduling solver -- v1.

Deliberately simple, per the agreed starting point:
  - No scoped/pinned rescheduling yet -- every currently queued/in_progress
    order is re-solved against every available machine on each run. Scoping
    (only touching what a specific disruption actually affects) is a planned
    refinement, not implemented here.
  - Objective: minimize total tardiness (weighted higher for rush orders),
    not yet balancing changeover cost or utilization -- those can be added
    as additional weighted terms once this baseline is validated.
  - Horizon: rest of the current shift (HORIZON_MINUTES).

KNOWN SIMPLIFICATIONS (carried over from data gaps found while building this):
  - No order-to-machine compatibility constraint exists in the data, so every
    available machine is treated as a valid candidate for every order.
  - Changeover cost is the flat heuristic from ml/changeover_cost.py, added
    additively to each job's duration -- not modeled as a proper
    sequence-dependent gap between consecutive jobs on a machine.
  - Orders without a real due_date (see solver/state.py) get a due date far
    outside the horizon, so they simply never generate tardiness pressure in
    this solve -- this is intentional, not a bug: we shouldn't invent urgency
    for data we don't actually have.
  - Order count is capped (see MAX_ORDERS) to keep the model tractable and
    avoid infeasibility from oversubscribing available machines -- a real
    scoping/prioritization strategy replaces this cap later.
"""

import os
import sys

import joblib
import pandas as pd
from ortools.sat.python import cp_model

# Relative to THIS FILE's location, not the current working directory --
# "../ml" alone would silently resolve to the wrong place if this script is
# ever run from somewhere other than inside solver/.
_ML_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ml")
sys.path.insert(0, _ML_DIR)
from changeover_cost import estimate_changeover_cost  # noqa: E402
from features import encode_type_for_inference  # noqa: E402

from state import fetch_current_machines, fetch_current_orders, fetch_reference_now

HORIZON_MINUTES = 600  # rest of shift + buffer
MAX_ORDERS = 30  # v1 cap -- see module docstring
RUSH_WEIGHT = 3
NORMAL_WEIGHT = 1
SOLVE_TIME_LIMIT_SECONDS = 30

CYCLE_TIME_MODEL_PATH = os.path.join(_ML_DIR, "models", "cycle_time_model.joblib")


def load_cycle_time_model():
    bundle = joblib.load(CYCLE_TIME_MODEL_PATH)
    return bundle["model"], bundle["feature_columns"]


def predict_duration_minutes(model, feature_columns, machine_type: str) -> int:
    """ML-predicted run time + heuristic changeover cost, for one order on one machine."""
    X = encode_type_for_inference(machine_type, feature_columns)
    predicted_run_minutes = float(model.predict(X)[0])
    changeover_minutes = estimate_changeover_cost(machine_type)
    total = max(1, round(predicted_run_minutes + changeover_minutes))
    return total


def select_orders(orders_df: pd.DataFrame) -> pd.DataFrame:
    """
    v1 order selection: rush orders first, then earliest due date, capped at
    MAX_ORDERS. This is a stand-in for real scoping logic -- see module
    docstring.
    """
    ordered = orders_df.copy()
    ordered["priority_rank"] = ordered["priority"].map({"rush": 0, "normal": 1}).fillna(1)
    ordered = ordered.sort_values(by=["priority_rank", "due_date"])
    return ordered.head(MAX_ORDERS)


def build_and_solve(machines_df: pd.DataFrame, orders_df: pd.DataFrame, model, feature_columns, reference_now: pd.Timestamp):
    now = reference_now
    cp = cp_model.CpModel()

    available_machines = machines_df[machines_df["status"] != "down"]
    if available_machines.empty:
        raise RuntimeError("No available machines -- every machine is currently down.")
    if orders_df.empty:
        raise RuntimeError("No queued/in_progress orders to schedule.")

    order_end = {}
    tardiness = {}
    assign_literals = {}
    machine_intervals = {m: [] for m in available_machines["machine_id"]}

    for _, order in orders_df.iterrows():
        oid = order["order_id"]
        due_minutes = (order["due_date"] - now).total_seconds() / 60
        due_minutes = int(max(0, min(HORIZON_MINUTES, due_minutes)))

        end_var = cp.NewIntVar(0, HORIZON_MINUTES, f"end_{oid}")
        order_end[oid] = end_var
        literals_for_order = []

        for _, machine in available_machines.iterrows():
            mid = machine["machine_id"]
            duration = predict_duration_minutes(model, feature_columns, machine["type"])

            lit = cp.NewBoolVar(f"assign_{oid}_{mid}")
            start = cp.NewIntVar(0, HORIZON_MINUTES, f"start_{oid}_{mid}")
            end = cp.NewIntVar(0, HORIZON_MINUTES, f"end_{oid}_{mid}")
            interval = cp.NewOptionalIntervalVar(start, duration, end, lit, f"interval_{oid}_{mid}")

            machine_intervals[mid].append(interval)
            cp.Add(order_end[oid] == end).OnlyEnforceIf(lit)
            literals_for_order.append(lit)
            assign_literals[(oid, mid)] = lit

        cp.AddExactlyOne(literals_for_order)

        tardy = cp.NewIntVar(0, HORIZON_MINUTES, f"tardy_{oid}")
        cp.Add(tardy >= order_end[oid] - due_minutes)
        cp.Add(tardy >= 0)
        tardiness[oid] = tardy

    for mid, intervals in machine_intervals.items():
        if intervals:
            cp.AddNoOverlap(intervals)

    def weight(priority):
        return RUSH_WEIGHT if priority == "rush" else NORMAL_WEIGHT

    cp.Minimize(
        sum(
            weight(order["priority"]) * tardiness[order["order_id"]]
            for _, order in orders_df.iterrows()
        )
    )

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = SOLVE_TIME_LIMIT_SECONDS
    status = solver.Solve(cp)

    return solver, status, assign_literals, order_end, tardiness


def main():
    print("Loading current state from ClickHouse ...")
    reference_now = fetch_reference_now()
    print(f"Using data-derived reference time: {reference_now} (not real wall-clock time -- see state.py)")

    machines_df = fetch_current_machines()
    orders_df = fetch_current_orders(reference_now)
    orders_df = select_orders(orders_df)
    print(f"{len(machines_df)} machines known, {len(orders_df)} orders selected for this solve "
          f"(capped at {MAX_ORDERS}).")

    real_due_dates = orders_df["has_real_due_date"].sum()
    print(f"{real_due_dates}/{len(orders_df)} selected orders have a real due date; "
          f"the rest use the fallback and won't generate tardiness pressure.")

    print("Loading cycle-time model ...")
    model, feature_columns = load_cycle_time_model()

    print("Building and solving ...")
    solver, status, assign_literals, order_end, tardiness = build_and_solve(
        machines_df, orders_df, model, feature_columns, reference_now
    )

    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        print(f"No solution found. Solver status: {solver.StatusName(status)}")
        return

    print(f"\nSolver status: {solver.StatusName(status)}  "
          f"(objective = {solver.ObjectiveValue()} weighted tardiness-minutes)\n")

    print(f"{'order_id':<12}{'machine_id':<12}{'end_min':<10}{'tardy_min':<10}")
    for (oid, mid), lit in assign_literals.items():
        if solver.Value(lit):
            print(f"{oid:<12}{mid:<12}{solver.Value(order_end[oid]):<10}{solver.Value(tardiness[oid]):<10}")


if __name__ == "__main__":
    main()
