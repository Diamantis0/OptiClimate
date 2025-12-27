# opticlimate/report/aggregate_loss.py

from __future__ import annotations

import pandas as pd

from opticlimate.report.schemas import LIMITING_PARAM_UNKNOWN
from opticlimate.report.time_features import extract_time_features


def _as_bool(s: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(s) or pd.api.types.is_boolean_dtype(s):
        return s.fillna(False).astype(bool)
    return s.fillna(False).astype(bool)


def loss_by_param_monthly(
    df: pd.DataFrame,
    *,
    time_local_col: str = "time_local",
    limiting_col: str = "limiting_param",
    operational_col: str = "is_operational",
    workable_col: str = "is_workable",
    scenario_col: str = "scenario_id",
) -> pd.DataFrame:
    """Decompose weather-lost hours by limiting parameter, grouped monthly.

    Rows considered are those where:
      is_operational == True AND is_workable == False

    Output columns:
      year, month, param, hours_lost, pct_of_weather_lost_hours, pct_of_operational_hours

    Notes
    - If limiting_col is null for a blocked hour, it is mapped to "unknown".
    - Percent metrics are fractions in [0,1].
    """
    if limiting_col not in df.columns:
        raise ValueError(f"Missing limiting_col {limiting_col!r}")

    dfx = extract_time_features(df, time_local_col=time_local_col)
    op = _as_bool(dfx[operational_col])
    wk = _as_bool(dfx[workable_col])

    blocked = op & (~wk)
    if not blocked.any():
        return pd.DataFrame(
            columns=[
                "year",
                "month",
                "param",
                "hours_lost",
                "pct_of_weather_lost_hours",
                "pct_of_operational_hours",
            ]
        )

    base_dict = {
        "year": dfx.loc[blocked, "year"],
        "month": dfx.loc[blocked, "month"],
        "param": dfx.loc[blocked, limiting_col].fillna(LIMITING_PARAM_UNKNOWN).astype(str),
    }
    if scenario_col in dfx.columns:
        base_dict[scenario_col] = dfx.loc[blocked, scenario_col]
    base = pd.DataFrame(base_dict)

    # hours lost by param
    group_cols = ["year", "month", "param"]
    if scenario_col in base.columns:
        group_cols = [scenario_col, *group_cols]

    out = (
        base.groupby(group_cols, as_index=False)
        .size()
        .rename(columns={"size": "hours_lost"})
        .sort_values(([scenario_col] if scenario_col in base.columns else []) + ["year", "month", "hours_lost"],
                     ascending=([True] if scenario_col in base.columns else []) + [True, True, False])
        .reset_index(drop=True)
    )

    # denominators per (year, month)
    den_dict = {
        "year": dfx["year"],
        "month": dfx["month"],
        "_op": op,
        "_blocked": blocked,
    }
    if scenario_col in dfx.columns:
        den_dict[scenario_col] = dfx[scenario_col]
    den = pd.DataFrame(den_dict)

    den_group_cols = ["year", "month"]
    if scenario_col in den.columns:
        den_group_cols = [scenario_col, *den_group_cols]

    den = den.groupby(den_group_cols, as_index=False).agg(
        operational_hours=("_op", "sum"),
        weather_lost_hours=("_blocked", "sum"),
    )

    merge_cols = ["year", "month"]
    if scenario_col in out.columns and scenario_col in den.columns:
        merge_cols = [scenario_col, *merge_cols]
    out = out.merge(den, on=merge_cols, how="left")
    out["pct_of_weather_lost_hours"] = (
        out["hours_lost"] / out["weather_lost_hours"].where(out["weather_lost_hours"] != 0, pd.NA)
    ).fillna(0.0)
    out["pct_of_operational_hours"] = (
        out["hours_lost"] / out["operational_hours"].where(out["operational_hours"] != 0, pd.NA)
    ).fillna(0.0)

    out = out.drop(columns=["operational_hours", "weather_lost_hours"])
    return out


def loss_by_param_yearly(
    df: pd.DataFrame,
    *,
    time_local_col: str = "time_local",
    limiting_col: str = "limiting_param",
    operational_col: str = "is_operational",
    workable_col: str = "is_workable",
    scenario_col: str = "scenario_id",
) -> pd.DataFrame:
    """Same as loss_by_param_monthly, but grouped by year."""
    if limiting_col not in df.columns:
        raise ValueError(f"Missing limiting_col {limiting_col!r}")

    dfx = extract_time_features(df, time_local_col=time_local_col)
    op = _as_bool(dfx[operational_col])
    wk = _as_bool(dfx[workable_col])
    blocked = op & (~wk)

    if not blocked.any():
        return pd.DataFrame(
            columns=[
                "year",
                "param",
                "hours_lost",
                "pct_of_weather_lost_hours",
                "pct_of_operational_hours",
            ]
        )

    base_dict = {
        "year": dfx.loc[blocked, "year"],
        "param": dfx.loc[blocked, limiting_col].fillna(LIMITING_PARAM_UNKNOWN).astype(str),
    }
    if scenario_col in dfx.columns:
        base_dict[scenario_col] = dfx.loc[blocked, scenario_col]
    base = pd.DataFrame(base_dict)

    group_cols = ["year", "param"]
    if scenario_col in base.columns:
        group_cols = [scenario_col, *group_cols]

    out = (
        base.groupby(group_cols, as_index=False)
        .size()
        .rename(columns={"size": "hours_lost"})
        .sort_values(([scenario_col] if scenario_col in base.columns else []) + ["year", "hours_lost"],
                     ascending=([True] if scenario_col in base.columns else []) + [True, False])
        .reset_index(drop=True)
    )

    den_dict = {"year": dfx["year"], "_op": op, "_blocked": blocked}
    if scenario_col in dfx.columns:
        den_dict[scenario_col] = dfx[scenario_col]
    den = pd.DataFrame(den_dict)
    den_group_cols = ["year"]
    if scenario_col in den.columns:
        den_group_cols = [scenario_col, *den_group_cols]
    den = den.groupby(den_group_cols, as_index=False).agg(
        operational_hours=("_op", "sum"),
        weather_lost_hours=("_blocked", "sum"),
    )

    merge_cols = ["year"]
    if scenario_col in out.columns and scenario_col in den.columns:
        merge_cols = [scenario_col, *merge_cols]
    out = out.merge(den, on=merge_cols, how="left")
    out["pct_of_weather_lost_hours"] = (
        out["hours_lost"] / out["weather_lost_hours"].where(out["weather_lost_hours"] != 0, pd.NA)
    ).fillna(0.0)
    out["pct_of_operational_hours"] = (
        out["hours_lost"] / out["operational_hours"].where(out["operational_hours"] != 0, pd.NA)
    ).fillna(0.0)

    out = out.drop(columns=["operational_hours", "weather_lost_hours"])
    return out
