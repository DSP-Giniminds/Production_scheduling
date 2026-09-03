"""
Changeover cost -- deliberately NOT a trained model.

Why: training a real model requires observing "job A ran, then job B ran on
the same machine, here's what that transition cost." The current schema has
no field linking a specific work order to the machine it ran on -- machine
events only carry a machine `type` (CNC, Press, etc.), never which order was
running. Without that link, there's no historical changeover to learn from.

To make this trainable later: generator.py's machine events (or a new event)
would need to carry the order_id that was running, so consecutive runs on the
same machine can be compared to derive an actual observed changeover time.

Until that data exists, this returns a flat heuristic constant per machine
type -- good enough to give the solver *something* to optimize against, but
explicitly not learned from history. Do not present this as a prediction.
"""

DEFAULT_CHANGEOVER_MINUTES = {
    "CNC": 15,
    "Press": 20,
    "Assembly": 10,
    "Paint": 25,
    "Weld": 15,
}

FALLBACK_MINUTES = 15


def estimate_changeover_cost(machine_type: str) -> float:
    """Flat heuristic, not a trained prediction. See module docstring."""
    return DEFAULT_CHANGEOVER_MINUTES.get(machine_type, FALLBACK_MINUTES)
