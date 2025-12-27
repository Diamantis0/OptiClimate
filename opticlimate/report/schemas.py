# opticlimate/report/schemas.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Sequence, Tuple

import pandas as pd


# =============================================================================
# Global conventions (locked)
# =============================================================================

# Context enum (strings) used across all stats outputs
CONTEXT_ALL = "all"
CONTEXT_OPERATIONAL = "operational"
CONTEXT_WORKABLE = "workable"
CONTEXT_BLOCKED_OPERATIONAL = "blocked_operational"

CONTEXTS: Tuple[str, ...] = (
    CONTEXT_ALL,
    CONTEXT_OPERATIONAL,
    CONTEXT_WORKABLE,
    CONTEXT_BLOCKED_OPERATIONAL,
)

# Quantiles (locked list)
QUANTILES: Tuple[str, ...] = ("p05", "p10", "p25", "p50", "p75", "p90", "p95")

# =============================================================================
# Canonical table names
# =============================================================================

# Core summaries
TABLE_SUMMARY_RUN = "summary_run"
TABLE_SUMMARY_MONTHLY = "summary_monthly"
TABLE_SUMMARY_YEARLY = "summary_yearly"

# Loss decomposition
TABLE_LOSS_BY_PARAM_MONTHLY = "loss_by_param_monthly"
TABLE_LOSS_BY_PARAM_YEARLY = "loss_by_param_yearly"

# Weather statistics
TABLE_WEATHER_STATS_MONTHLY = "weather_stats_monthly"
TABLE_WEATHER_STATS_YEARLY = "weather_stats_yearly"

# Curves (Phase 5D)
TABLE_CURVES_CUMULATIVE_DAILY = "curves_cumulative_daily"
TABLE_CURVE_RELIABILITY_MONTHLY = "curve_reliability_monthly"

# Visual-ready pivots (Phase 5E)
TABLE_PIVOT_MONTH_X_HOUR_WORKABLE_RATE = "pivot_month_x_hour_workable_rate"
TABLE_PIVOT_MONTH_X_WEEKDAY_WORKABLE_RATE = "pivot_month_x_weekday_workable_rate"
TABLE_PIVOT_WEEKDAY_X_HOUR_WORKABLE_RATE = "pivot_weekday_x_hour_workable_rate"
TABLE_PIVOT_MONTH_X_HOUR_PARAM_MEAN_OPERATIONAL = "pivot_month_x_hour_param_mean_operational"

# Statistics (Phase 5F)
TABLE_STATS_MONTHLY_WORKABLE_RATE_DIST = "stats_monthly_workable_rate_dist"
TABLE_RELIABILITY_TARGETS_MONTHLY = "reliability_targets_monthly"

LIMITING_PARAM_UNKNOWN = "unknown"

STREAKS_NONWORKABLE_OPERATIONAL = "streaks_nonworkable_operational"
STREAKS_WORKABLE_OPERATIONAL = "streaks_workable_operational"
STREAKS_SUMMARY_OPERATIONAL = "streaks_summary_operational"


# =============================================================================
# Hourly truth table schema
# =============================================================================

HOURLY_REQUIRED_COLUMNS: Tuple[str, ...] = (
    "time_utc",
    "time_local",
    "is_operational",
    "is_workable",
    "limiting_param",
)


# =============================================================================
# Aggregation bundle
# =============================================================================

@dataclass(frozen=True)
class AggregationBundle:
    """
    Standard aggregation output container.

    - meta must be JSON-serializable
    - tables maps canonical table names to DataFrames
    """
    meta: Mapping[str, Any]
    tables: Mapping[str, pd.DataFrame]


# =============================================================================
# Validators
# =============================================================================

class SchemaError(ValueError):
    pass


def validate_hourly_truth_table(
    df: pd.DataFrame,
    *,
    weather_params: Sequence[str] | None = None,
) -> None:
    if df.empty:
        raise SchemaError("Hourly dataframe is empty")

    missing = [c for c in HOURLY_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise SchemaError(f"Hourly dataframe missing required columns: {missing}")

    if not pd.api.types.is_datetime64_any_dtype(df["time_local"]):
        raise SchemaError("time_local must be datetime dtype")

    if not pd.api.types.is_datetime64_any_dtype(df["time_utc"]):
        raise SchemaError("time_utc must be datetime dtype")

    if weather_params:
        missing_params = [p for p in weather_params if p not in df.columns]
        if missing_params:
            raise SchemaError(f"Missing weather parameter columns: {missing_params}")


def validate_bundle(
    bundle: AggregationBundle,
    *,
    require: Sequence[str],
) -> None:
    if not isinstance(bundle.meta, Mapping):
        raise SchemaError("bundle.meta must be a mapping")

    if not isinstance(bundle.tables, Mapping):
        raise SchemaError("bundle.tables must be a mapping")

    missing = [k for k in require if k not in bundle.tables]
    if missing:
        raise SchemaError(f"AggregationBundle missing required tables: {missing}")

    # Optional sanity checks for rates
    for name, df in bundle.tables.items():
        if not isinstance(df, pd.DataFrame):
            raise SchemaError(f"Table {name!r} is not a DataFrame")

        for col in df.columns:
            if "_pct_" in col or col.endswith("_rate"):
                s = df[col].dropna()
                if not s.empty and ((s < 0).any() or (s > 1).any()):
                    raise SchemaError(
                        f"Table {name!r} column {col!r} has values outside [0,1]"
                    )


# =============================================================================
# Meta helper
# =============================================================================

def make_meta_skeleton(
    *,
    run_id: str,
    run_id_raw: str | None = None,
    run_id_sanitized: str | None = None,
    generated_at_utc: str,
    location_name: str,
    timezone: str,
    period_start_utc: str,
    period_end_utc: str,
    parameters: Sequence[str],
    thresholds: Mapping[str, Any],
    config_hash: str,
    rows_hourly: int,
) -> Dict[str, Any]:
    meta = {
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "location_name": location_name,
        "timezone": timezone,
        "period_start_utc": period_start_utc,
        "period_end_utc": period_end_utc,
        "parameters": list(parameters),
        "thresholds": dict(thresholds),
        "config_hash": config_hash,
        "rows_hourly": int(rows_hourly),
    }

    if run_id_raw is not None:
        meta["run_id_raw"] = run_id_raw
    if run_id_sanitized is not None:
        meta["run_id_sanitized"] = run_id_sanitized

    return meta
