# opticlimate/report/aggregate_visual.py

from __future__ import annotations

from typing import Sequence

import numpy as np
import pandas as pd

from opticlimate.report.time_features import extract_time_features


def _workable_rate_pivot(
    df: pd.DataFrame,
    *,
    time_local_col: str,
    operational_col: str,
    workable_col: str,
    dims: Sequence[str],
    scenario_col: str = "scenario_id",
) -> pd.DataFrame:
    """
    Generic flat pivot builder for workable rate within operational hours.
    Returns a flat table with dims + metrics:
      - workable_pct_of_operational_hours (fraction)
      - operational_hours
      - total_hours
    """
    dfx = extract_time_features(df, time_local_col=time_local_col)

    is_op = dfx[operational_col].astype(bool)
    is_work = dfx[workable_col].astype(bool)

    _dims = list(dims)
    if scenario_col in dfx.columns and scenario_col not in _dims:
        _dims = [scenario_col, *_dims]

    g = (
        dfx.assign(
            total_hours=1,
            operational_hours=is_op.astype(int),
            workable_hours=(is_op & is_work).astype(int),
        )
        .groupby(_dims, as_index=False)[["total_hours", "operational_hours", "workable_hours"]]
        .sum()
    )

    g["workable_pct_of_operational_hours"] = np.where(
        g["operational_hours"] > 0,
        g["workable_hours"] / g["operational_hours"],
        np.nan,
    )

    # keep schema-required metrics
    out = g.drop(columns=["workable_hours"]).sort_values(_dims).reset_index(drop=True)
    return out


def pivot_month_x_hour_workable_rate(
    df: pd.DataFrame,
    *,
    time_local_col: str = "time_local",
    operational_col: str = "is_operational",
    workable_col: str = "is_workable",
    scenario_col: str = "scenario_id",
) -> pd.DataFrame:
    return _workable_rate_pivot(
        df,
        time_local_col=time_local_col,
        operational_col=operational_col,
        workable_col=workable_col,
        dims=("month", "hour"),
        scenario_col=scenario_col,
    )


def pivot_month_x_weekday_workable_rate(
    df: pd.DataFrame,
    *,
    time_local_col: str = "time_local",
    operational_col: str = "is_operational",
    workable_col: str = "is_workable",
    scenario_col: str = "scenario_id",
) -> pd.DataFrame:
    return _workable_rate_pivot(
        df,
        time_local_col=time_local_col,
        operational_col=operational_col,
        workable_col=workable_col,
        dims=("month", "weekday"),
        scenario_col=scenario_col,
    )


def pivot_weekday_x_hour_workable_rate(
    df: pd.DataFrame,
    *,
    time_local_col: str = "time_local",
    operational_col: str = "is_operational",
    workable_col: str = "is_workable",
    scenario_col: str = "scenario_id",
) -> pd.DataFrame:
    return _workable_rate_pivot(
        df,
        time_local_col=time_local_col,
        operational_col=operational_col,
        workable_col=workable_col,
        dims=("weekday", "hour"),
        scenario_col=scenario_col,
    )


def pivot_month_x_hour_param_mean_operational(
    df: pd.DataFrame,
    *,
    params: Sequence[str],
    time_local_col: str = "time_local",
    operational_col: str = "is_operational",
    scenario_col: str = "scenario_id",
) -> pd.DataFrame:
    """
    Flat table for heatmaps like: month x hour mean wind_speed (during operational hours).
    Output dims: month, hour, param
    Metrics: n_hours, missing_rate, mean

    Notes:
    - We compute using operational hours only (planning context).
    - mean ignores NaNs; missing_rate computed within operational subset.
    """
    dfx = extract_time_features(df, time_local_col=time_local_col)
    is_op = dfx[operational_col].astype(bool)

    # Keep only operational hours for planning
    op_df = dfx.loc[is_op].copy()

    rows = []
    for p in params:
        if p not in op_df.columns:
            raise ValueError(f"Missing weather parameter column: {p}")

        group_cols = ["month", "hour"]
        if scenario_col in op_df.columns:
            group_cols = [scenario_col, *group_cols]
        grp = op_df.groupby(group_cols, as_index=False)[p].agg(["count", "size", "mean"]).reset_index()
        # count = non-null count, size = total rows in group
        grp["n_hours"] = grp["size"].astype("int64")
        grp["missing_rate"] = np.where(grp["size"] > 0, 1.0 - (grp["count"] / grp["size"]), np.nan)
        grp = grp.rename(columns={"mean": "mean"})
        grp["param"] = p

        keep_cols = ["month", "hour", "param", "n_hours", "missing_rate", "mean"]
        if scenario_col in grp.columns:
            keep_cols = [scenario_col, *keep_cols]
        rows.append(grp[keep_cols])

    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(
        columns=["month", "hour", "param", "n_hours", "missing_rate", "mean"]
    )
    sort_cols = ["param", "month", "hour"]
    if scenario_col in out.columns:
        sort_cols = [scenario_col, *sort_cols]
    out = out.sort_values(sort_cols).reset_index(drop=True)
    return out
