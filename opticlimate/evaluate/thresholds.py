# opticlimate/evaluate/thresholds.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class ScenarioEvaluation:
    scenario: str
    is_workable: pd.Series
    param_ok: Dict[str, pd.Series]          # per-param boolean series
    limiting_param: Optional[pd.Series]     # name of first failing param per row (or None)


@dataclass(frozen=True)
class EvaluationSummary:
    total_hours: int
    operational_hours: int
    workable_hours: int
    workable_pct_of_operational: float
    workable_pct_of_total: float


def _bounds_for(weather_thresholds: Dict[str, Any], param: str, scenario: str) -> Tuple[Optional[float], Optional[float]]:
    """Return (min, max) bounds for a given (scenario, param) from *scenario-first* thresholds.

    Required shape:
        weather_thresholds[scenario][param] -> {min?, max?}

    Bounds are optional; missing bounds return (None, None).

    This is intentionally strict (no legacy param-first fallback).
    """
    if not isinstance(weather_thresholds, dict):
        return None, None

    per_scen = weather_thresholds.get(scenario, {})
    if not isinstance(per_scen, dict):
        return None, None

    bounds = per_scen.get(param, {})
    if not isinstance(bounds, dict):
        return None, None

    mn = bounds.get("min")
    mx = bounds.get("max")

    mn_f = float(mn) if isinstance(mn, (int, float)) else None
    mx_f = float(mx) if isinstance(mx, (int, float)) else None
    return mn_f, mx_f


def evaluate_thresholds(
    df: pd.DataFrame,
    required_parameters: list[str],
    weather_thresholds: Dict[str, Any],
    scenario: str = "base",
    operational_col: str = "is_operational",
) -> Tuple[ScenarioEvaluation, EvaluationSummary]:
    """
    Evaluate min/max thresholds for required_parameters under a given scenario.

    Rules:
      - If a parameter has no thresholds for the scenario: it is treated as always OK.
      - If min is set: value >= min
      - If max is set: value <= max
      - Workable = is_operational AND all params OK
    """
    if operational_col not in df.columns:
        raise ValueError(f"Missing required column {operational_col!r} in df")

    op = df[operational_col].astype(bool)
    total_hours = int(len(df))
    operational_hours = int(op.sum())

    param_ok: Dict[str, pd.Series] = {}

    # Build per-param ok masks
    for p in required_parameters:
        if p not in df.columns:
            raise ValueError(f"DataFrame missing required weather parameter column {p!r}")

        mn, mx = _bounds_for(weather_thresholds, p, scenario)
        s = df[p]

        ok = pd.Series(True, index=df.index)
        if mn is not None:
            ok = ok & (s >= float(mn))
        if mx is not None:
            ok = ok & (s <= float(mx))

        param_ok[p] = ok.astype(bool)

    # All params ok
    if param_ok:
        all_ok = pd.concat(param_ok.values(), axis=1).all(axis=1)
    else:
        all_ok = pd.Series(True, index=df.index)

    is_workable = (op & all_ok).astype(bool)

    # Optional: identify first failing param for debugging / reporting
    limiting = None
    if param_ok:
        # first param in required_parameters order that fails
        fail_matrix = pd.DataFrame({p: ~param_ok[p] for p in required_parameters})
        any_fail = fail_matrix.any(axis=1)
        limiting = pd.Series(None, index=df.index, dtype="object")
        for p in required_parameters:
            limiting.loc[any_fail & fail_matrix[p]] = p
            # once set, don't overwrite
            any_fail = any_fail & limiting.isna()

    workable_hours = int(is_workable.sum())
    workable_pct_of_operational = (workable_hours / operational_hours * 100.0) if operational_hours else 0.0
    workable_pct_of_total = (workable_hours / total_hours * 100.0) if total_hours else 0.0

    summary = EvaluationSummary(
        total_hours=total_hours,
        operational_hours=operational_hours,
        workable_hours=workable_hours,
        workable_pct_of_operational=workable_pct_of_operational,
        workable_pct_of_total=workable_pct_of_total,
    )

    result = ScenarioEvaluation(
        scenario=scenario,
        is_workable=is_workable,
        param_ok=param_ok,
        limiting_param=limiting,
    )
    return result, summary
