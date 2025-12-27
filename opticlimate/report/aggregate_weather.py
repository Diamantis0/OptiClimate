# opticlimate/report/aggregate_weather.py

from __future__ import annotations

from typing import Dict, List, Sequence, Tuple

import pandas as pd

from opticlimate.report import schemas
from opticlimate.report.time_features import extract_time_features


_QUANTILE_MAP: Dict[float, str] = {
    0.05: "p05",
    0.10: "p10",
    0.25: "p25",
    0.50: "p50",
    0.75: "p75",
    0.90: "p90",
    0.95: "p95",
}


def _ensure_time_features(df: pd.DataFrame, time_local_col: str) -> pd.DataFrame:
    """Ensure year/month/day/date_local/weekday/hour/week_of_year exist."""
    if all(c in df.columns for c in schemas.TIME_FEATURE_REQUIRED_COLUMNS):
        return df
    return extract_time_features(df, time_local_col=time_local_col)


def _split_contexts(
    df: pd.DataFrame,
    *,
    operational_col: str,
    workable_col: str,
) -> List[Tuple[str, pd.DataFrame]]:
    """Return (context, frame) pairs."""
    # All hours
    all_df = df
    op_mask = df[operational_col] == True
    wk_mask = df[workable_col] == True
    blocked_mask = op_mask & (~wk_mask)

    return [
        (schemas.CONTEXT_ALL, all_df),
        (schemas.CONTEXT_OPERATIONAL, df.loc[op_mask]),
        (schemas.CONTEXT_WORKABLE, df.loc[wk_mask]),
        (schemas.CONTEXT_BLOCKED_OPERATIONAL, df.loc[blocked_mask]),
    ]


def _weather_stats_long(
    df: pd.DataFrame,
    *,
    group_dims: Sequence[str],
    params: Sequence[str],
    context: str,
) -> pd.DataFrame:
    """Compute long-form stats for a single context.

    Output columns:
      group_dims + ['context', 'param'] + required metrics (see schemas)
    """
    out_parts: List[pd.DataFrame] = []
    keys = list(group_dims) + ["param", "context"]

    for param in params:
        if param not in df.columns:
            # This is a configuration/data mismatch; fail fast so callers notice.
            raise ValueError(f"Weather parameter column '{param}' not found in dataframe")

        base = df[list(group_dims) + [param]].rename(columns={param: "value"}).copy()
        base["param"] = param
        base["context"] = context
        base["is_missing"] = base["value"].isna()

        g = base.groupby(keys, dropna=False)

        n_hours = g.size().rename("n_hours")
        missing_rate = g["is_missing"].mean().rename("missing_rate")

        core = g["value"].agg(["mean", "std", "min", "max"])

        # Quantiles: returns multiindex series with quantile level
        qs = list(_QUANTILE_MAP.keys())
        q = g["value"].quantile(qs)
        q = q.unstack(-1)  # quantiles become columns
        q = q.rename(columns=_QUANTILE_MAP)

        # Ensure all expected quantile columns exist
        for col in schemas.QUANTILES:
            if col not in q.columns:
                q[col] = pd.NA

        assembled = pd.concat([n_hours, missing_rate, core, q[schemas.QUANTILES]], axis=1).reset_index()
        # Column order per schema
        assembled = assembled[list(group_dims) + ["param", "context", "n_hours", "missing_rate", "mean", "std", "min", *schemas.QUANTILES, "max"]]
        out_parts.append(assembled)

    if not out_parts:
        return pd.DataFrame(columns=list(group_dims) + ["param", "context", *schemas.WEATHER_STATS_METRICS_REQUIRED])

    return pd.concat(out_parts, ignore_index=True)


def weather_stats_monthly(
    hourly: pd.DataFrame,
    *,
    params: Sequence[str],
    time_local_col: str = "time_local",
    operational_col: str = "is_operational",
    workable_col: str = "is_workable",
    scenario_col: str = "scenario_id",
) -> pd.DataFrame:
    """Compute monthly weather statistics by context and parameter (long-form)."""
    if not params:
        return pd.DataFrame(columns=[*schemas.WEATHER_STATS_DIMENSIONS_MONTHLY, *schemas.WEATHER_STATS_METRICS_REQUIRED])

    df = _ensure_time_features(hourly, time_local_col=time_local_col)
    group_dims = ["year", "month"]
    if scenario_col in df.columns:
        group_dims = [scenario_col, *group_dims]

    parts: List[pd.DataFrame] = []
    for context, sub in _split_contexts(df, operational_col=operational_col, workable_col=workable_col):
        part = _weather_stats_long(sub, group_dims=group_dims, params=params, context=context)
        parts.append(part)

    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    # Normalize column order (scenario-aware)
    cols = ["year", "month"]
    if scenario_col in out.columns:
        cols = [scenario_col, *cols]
    cols = [*cols, "context", "param", *schemas.WEATHER_STATS_METRICS_REQUIRED]
    out = out[cols]
    return out


def weather_stats_yearly(
    hourly: pd.DataFrame,
    *,
    params: Sequence[str],
    time_local_col: str = "time_local",
    operational_col: str = "is_operational",
    workable_col: str = "is_workable",
    scenario_col: str = "scenario_id",
) -> pd.DataFrame:
    """Compute yearly weather statistics by context and parameter (long-form)."""
    if not params:
        return pd.DataFrame(columns=[*schemas.WEATHER_STATS_DIMENSIONS_YEARLY, *schemas.WEATHER_STATS_METRICS_REQUIRED])

    df = _ensure_time_features(hourly, time_local_col=time_local_col)
    group_dims = ["year"]
    if scenario_col in df.columns:
        group_dims = [scenario_col, *group_dims]

    parts: List[pd.DataFrame] = []
    for context, sub in _split_contexts(df, operational_col=operational_col, workable_col=workable_col):
        part = _weather_stats_long(sub, group_dims=group_dims, params=params, context=context)
        parts.append(part)

    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    cols = ["year"]
    if scenario_col in out.columns:
        cols = [scenario_col, *cols]
    cols = [*cols, "context", "param", *schemas.WEATHER_STATS_METRICS_REQUIRED]
    out = out[cols]
    return out
