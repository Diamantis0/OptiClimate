# opticlimate/classify/classify.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from opticlimate.temporal.operational_window import build_operational_mask_fixed_time
from opticlimate.evaluate.thresholds import evaluate_thresholds

LIMITING_PARAM_UNKNOWN = "unknown"


@dataclass(frozen=True)
class ClassificationResult:
    """Result of applying a scenario's operational policy + thresholds to truth data."""

    scenario_id: str
    classified_df: pd.DataFrame  # includes flags + limiting attribution
    # Keep raw evaluation around if callers need debugging; optional.
    evaluation_summary: Dict[str, Any]


def classify_baseline(
    truth_df: pd.DataFrame,
    cfg: Dict[str, Any],
    *,
    scenario_id: str = "base",
    time_local_col: str = "time_local",
    time_utc_col: str = "time_utc",
) -> ClassificationResult:
    """
    Apply the current OptiClimate V2 baseline logic as a *classification* step.

    Invariants enforced here:
      - scenario_id is attached
      - workable_flag implies operational_flag
      - loss_flag is operational_flag & ~workable_flag
      - limiting_param is only defined for loss hours; otherwise null
    """
    if time_local_col not in truth_df.columns:
        raise ValueError(f"truth_df missing required {time_local_col!r} column")
    if time_utc_col not in truth_df.columns:
        raise ValueError(f"truth_df missing required {time_utc_col!r} column")

    df = truth_df.copy()
    df["scenario_id"] = scenario_id

    # Operational mask (current platform uses fixed_time window)
    op_mask = build_operational_mask_fixed_time(
            df,
            cfg["operational_window"],
            time_col=time_local_col,
        )
    df["operational_flag"] = op_mask.is_operational.astype(bool)

    # Threshold evaluation - relies on cfg semantics already present in V2
    required_parameters: List[str] = list(cfg.get("required_parameters", []))
    weather_thresholds: Dict[str, Any] = cfg.get("weather_thresholds", {})

    # evaluate_thresholds expects an operational column name
    eval_out, eval_summary = evaluate_thresholds(
        df.rename(columns={"operational_flag": "is_operational"}),
        required_parameters=required_parameters,
        weather_thresholds=weather_thresholds,
        scenario=scenario_id,
        operational_col="is_operational",
    )

    # Workable flag: evaluation already includes operational AND all params OK.
    workable = eval_out.is_workable.astype(bool)
    # Enforce invariant (never workable outside operational)
    workable = workable & df["operational_flag"]
    df["workable_flag"] = workable

    df["loss_flag"] = df["operational_flag"] & (~df["workable_flag"])

    # Limiting attribution: only meaningful for loss hours (operational & not workable)
    limiting = eval_out.limiting_param
    if limiting is None:
        limiting = pd.Series(None, index=df.index, dtype="object")
    else:
        limiting = limiting.copy()

    # Null out outside loss hours
    limiting = limiting.where(df["loss_flag"], None)
    # For loss hours with no limiter, assign unknown
    limiting = limiting.mask(df["loss_flag"] & limiting.isna(), LIMITING_PARAM_UNKNOWN)
    df["limiting_param"] = limiting

    # Keep compatibility with prior naming used in downstream reports
    df["is_operational"] = df["operational_flag"]
    df["is_workable"] = df["workable_flag"]

    return ClassificationResult(
        scenario_id=scenario_id,
        classified_df=df,
        evaluation_summary={
            "operational_window_reason": op_mask.reason,
            "threshold_summary": {
                "total_hours": int(eval_summary.total_hours),
                "operational_hours": int(eval_summary.operational_hours),
                "workable_hours": int(eval_summary.workable_hours),
                "workable_pct_of_operational": float(eval_summary.workable_pct_of_operational),
                "workable_pct_of_total": float(eval_summary.workable_pct_of_total),
            },
        },
    )
