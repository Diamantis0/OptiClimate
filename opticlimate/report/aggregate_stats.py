# opticlimate/report/aggregate_stats.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

from opticlimate.report.time_features import extract_time_features


def stats_monthly_workable_rate_dist(
    df: pd.DataFrame,
    *,
    time_col: str = "time_local",
    scenario_col: str = "scenario_id",
    quantiles: Sequence[float] = (0.10, 0.25, 0.50, 0.75, 0.90),
) -> pd.DataFrame:
    """Distribution of monthly workable rate (workable / operational).

    Output columns:
      scenario_id (if present), n_months,
      mean_workable_rate, std_workable_rate,
      p10_workable_rate, p25_workable_rate, p50_workable_rate, p75_workable_rate, p90_workable_rate,
      min_workable_rate, max_workable_rate
    """
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

    monthly["workable_rate"] = np.where(
        monthly["operational"] > 0,
        monthly["workable"] / monthly["operational"],
        np.nan,
    )

    def _summarize(series: pd.Series) -> dict:
        s = series.dropna().astype(float)
        out: dict = {
            "n_months": int(s.shape[0]),
            "mean_workable_rate": float(s.mean()) if not s.empty else np.nan,
            "std_workable_rate": float(s.std(ddof=0)) if s.shape[0] > 0 else np.nan,
            "min_workable_rate": float(s.min()) if not s.empty else np.nan,
            "max_workable_rate": float(s.max()) if not s.empty else np.nan,
        }
        for q in quantiles:
            label = f"p{int(round(q*100)):02d}_workable_rate"
            out[label] = float(s.quantile(q)) if not s.empty else np.nan
        return out

    if scenario_col in monthly.columns:
        rows = []
        for scen, sub in monthly.groupby(scenario_col, dropna=False):
            row = {scenario_col: scen}
            row.update(_summarize(sub["workable_rate"]))
            rows.append(row)
        return pd.DataFrame(rows).sort_values([scenario_col]).reset_index(drop=True)

    return pd.DataFrame([_summarize(monthly["workable_rate"])])


def reliability_targets_monthly(
    df: pd.DataFrame,
    *,
    time_col: str = "time_local",
    scenario_col: str = "scenario_id",
    share_thresholds: Sequence[float] = (0.50, 0.60, 0.70, 0.80, 0.90),
    p_targets: Sequence[float] = (0.50, 0.80, 0.90),
) -> pd.DataFrame:
    """Compact reliability answers over months.

    Computes:
      - share of months where workable_rate >= each threshold
      - p50/p80/p90 of monthly workable_rate

    Output columns:
      scenario_id (if present), n_months,
      share_months_ge_50 ...,
      p50_monthly_workable_rate, p80_monthly_workable_rate, p90_monthly_workable_rate
    """
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

    monthly["workable_rate"] = np.where(
        monthly["operational"] > 0,
        monthly["workable"] / monthly["operational"],
        np.nan,
    )

    def _summarize(series: pd.Series) -> dict:
        s = series.dropna().astype(float)
        n = int(s.shape[0])
        out: dict = {"n_months": n}

        for t in share_thresholds:
            key = f"share_months_ge_{int(round(t*100))}"
            out[key] = float((s >= float(t)).mean()) if n > 0 else np.nan

        for p in p_targets:
            key = f"p{int(round(p*100))}_monthly_workable_rate"
            # quantile p gives non-exceedance; we use it as a planning statistic
            out[key] = float(s.quantile(p)) if n > 0 else np.nan

        return out

    if scenario_col in monthly.columns:
        rows = []
        for scen, sub in monthly.groupby(scenario_col, dropna=False):
            row = {scenario_col: scen}
            row.update(_summarize(sub["workable_rate"]))
            rows.append(row)
        return pd.DataFrame(rows).sort_values([scenario_col]).reset_index(drop=True)

    return pd.DataFrame([_summarize(monthly["workable_rate"])])
