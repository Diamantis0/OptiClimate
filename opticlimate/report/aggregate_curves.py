# opticlimate/report/aggregate_curves.py

from __future__ import annotations

from typing import Sequence

import numpy as np
import pandas as pd

from opticlimate.report.time_features import extract_time_features


def curves_cumulative_daily(
    df: pd.DataFrame,
    *,
    time_col: str = "time_local",
    scenario_col: str = "scenario_id",
) -> pd.DataFrame:
    """
    Build daily totals + cumulative totals for:
      - operational hours
      - workable hours
      - weather lost hours (operational & not workable)
      - schedule lost hours (not operational)

    Output schema (per day):
      date_local,
      operational_hours_daily, workable_hours_daily, weather_lost_hours_daily, schedule_lost_hours_daily,
      operational_hours_cum, workable_hours_cum, weather_lost_hours_cum, schedule_lost_hours_cum
    """
    if time_col not in df.columns:
        raise ValueError(f"Missing time column: {time_col}")
    for c in ("is_operational", "is_workable"):
        if c not in df.columns:
            raise ValueError(f"Missing required column: {c}")

    # NOTE: time_features.extract_time_features uses time_local_col (not time_col)
    dfx = extract_time_features(df, time_local_col=time_col)

    # Define hour categories
    is_operational = dfx["is_operational"].astype(bool)
    is_workable = dfx["is_workable"].astype(bool)

    operational = is_operational
    workable = is_operational & is_workable
    weather_lost = is_operational & (~is_workable)
    schedule_lost = ~is_operational

    # Daily sums (hourly rows => sum of boolean masks)
    group_cols = ["date_local"]
    if scenario_col in dfx.columns:
        group_cols = [scenario_col, *group_cols]

    daily = (
        dfx.assign(
            operational_hours_daily=operational.astype(int),
            workable_hours_daily=workable.astype(int),
            weather_lost_hours_daily=weather_lost.astype(int),
            schedule_lost_hours_daily=schedule_lost.astype(int),
        )
        .groupby(group_cols, as_index=False)[
            [
                "operational_hours_daily",
                "workable_hours_daily",
                "weather_lost_hours_daily",
                "schedule_lost_hours_daily",
            ]
        ]
        .sum()
        .sort_values(group_cols)
        .reset_index(drop=True)
    )

    # Cumulative sums
    if scenario_col in daily.columns:
        daily["operational_hours_cum"] = daily.groupby(scenario_col)["operational_hours_daily"].cumsum()
        daily["workable_hours_cum"] = daily.groupby(scenario_col)["workable_hours_daily"].cumsum()
        daily["weather_lost_hours_cum"] = daily.groupby(scenario_col)["weather_lost_hours_daily"].cumsum()
        daily["schedule_lost_hours_cum"] = daily.groupby(scenario_col)["schedule_lost_hours_daily"].cumsum()
    else:
        daily["operational_hours_cum"] = daily["operational_hours_daily"].cumsum()
        daily["workable_hours_cum"] = daily["workable_hours_daily"].cumsum()
        daily["weather_lost_hours_cum"] = daily["weather_lost_hours_daily"].cumsum()
        daily["schedule_lost_hours_cum"] = daily["schedule_lost_hours_daily"].cumsum()

    return daily


def curve_reliability_monthly(
    df: pd.DataFrame,
    *,
    time_col: str = "time_local",
    thresholds: Sequence[float] | None = None,
    scenario_col: str = "scenario_id",
) -> pd.DataFrame:
    """
    Reliability / exceedance curve:
      x-axis: workable_pct_of_operational_hours threshold (fraction)
      y-axis: fraction of months meeting or exceeding that threshold

    Uses per-month workable_pct_of_operational_hours = workable_hours / operational_hours.
    """
    if thresholds is None:
        # default thresholds from 0.50 to 1.00 step 0.05
        thresholds = tuple(np.round(np.arange(0.50, 1.0001, 0.05), 2))

    # NOTE: time_features.extract_time_features uses time_local_col (not time_col)
    dfx = extract_time_features(df, time_local_col=time_col)

    is_operational = dfx["is_operational"].astype(bool)
    is_workable = dfx["is_workable"].astype(bool)

    group_cols = ["year", "month"]
    if scenario_col in dfx.columns:
        group_cols = [scenario_col, *group_cols]

    monthly = (
        dfx.assign(
            operational=is_operational.astype(int),
            workable=(is_operational & is_workable).astype(int),
        )
        .groupby(group_cols, as_index=False)[["operational", "workable"]]
        .sum()
    )

    # fraction in [0,1]; if operational=0, define NaN
    monthly["workable_pct_of_operational_hours"] = np.where(
        monthly["operational"] > 0,
        monthly["workable"] / monthly["operational"],
        np.nan,
    )

    if scenario_col in monthly.columns:
        rows = []
        for scen, sub in monthly.groupby(scenario_col, dropna=False):
            valid = sub["workable_pct_of_operational_hours"].dropna()
            n_months = int(valid.shape[0])
            for t in thresholds:
                t = float(t)
                frac = float((valid >= t).mean()) if n_months > 0 else np.nan
                rows.append(
                    {
                        scenario_col: scen,
                        "threshold_workable_pct": t,
                        "fraction_of_months_meeting_threshold": frac,
                        "n_months": n_months,
                    }
                )
        return pd.DataFrame(rows).sort_values([scenario_col, "threshold_workable_pct"]).reset_index(drop=True)

    valid = monthly["workable_pct_of_operational_hours"].dropna()
    n_months = int(valid.shape[0])

    rows = []
    for t in thresholds:
        t = float(t)
        frac = float((valid >= t).mean()) if n_months > 0 else np.nan
        rows.append(
            {
                "threshold_workable_pct": t,
                "fraction_of_months_meeting_threshold": frac,
                "n_months": n_months,
            }
        )

    return pd.DataFrame(rows)
