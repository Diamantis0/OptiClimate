# opticlimate/report/aggregate_core.py

from __future__ import annotations

import pandas as pd

from opticlimate.report.time_features import extract_time_features


def _as_bool(s: pd.Series) -> pd.Series:
    """Robustly coerce a series to boolean values."""
    # pandas nullable boolean is fine; astype(bool) converts NaN->True/False
    # in surprising ways, so prefer fillna(False) first.
    if pd.api.types.is_bool_dtype(s) or pd.api.types.is_boolean_dtype(s):
        return s.fillna(False).astype(bool)
    return s.fillna(False).astype(bool)


def summary_run(
    df: pd.DataFrame,
    *,
    operational_col: str = "is_operational",
    workable_col: str = "is_workable",
    scenario_col: str = "scenario_id",
) -> pd.DataFrame:
    """Single-row run summary matching report.schemas.SUMMARY_METRICS_REQUIRED.

    All rates are fractions in [0,1].
    """
    def _summary_for(sub: pd.DataFrame) -> dict:
        total_hours = int(len(sub))
        op = int(_as_bool(sub[operational_col]).sum())
        wk = int(_as_bool(sub[workable_col]).sum())

        schedule_lost = total_hours - op
        weather_lost = op - wk

        def safe_div(num: float, den: float) -> float:
            return float(num / den) if den else 0.0

        return {
            "total_hours": total_hours,
            "operational_hours": op,
            "workable_hours": wk,
            "schedule_lost_hours": schedule_lost,
            "weather_lost_hours": weather_lost,
            "operational_pct_of_total_hours": safe_div(op, total_hours),
            "workable_pct_of_total_hours": safe_div(wk, total_hours),
            "workable_pct_of_operational_hours": safe_div(wk, op),
            "weather_lost_pct_of_operational_hours": safe_div(weather_lost, op),
            "schedule_lost_pct_of_total_hours": safe_div(schedule_lost, total_hours),
        }

    # Scenario-aware: if scenario column exists, output one row per scenario
    if scenario_col in df.columns:
        out_rows = []
        for scen, sub in df.groupby(scenario_col, dropna=False):
            row = _summary_for(sub)
            row[scenario_col] = scen
            out_rows.append(row)
        out = pd.DataFrame(out_rows)
        return out.sort_values([scenario_col]).reset_index(drop=True)

    return pd.DataFrame([_summary_for(df)])


def summary_monthly(
    df: pd.DataFrame,
    *,
    time_local_col: str = "time_local",
    operational_col: str = "is_operational",
    workable_col: str = "is_workable",
    scenario_col: str = "scenario_id",
    drop_empty: bool = True,
) -> pd.DataFrame:
    """Monthly summary grouped by (year, month)."""
    dfx = extract_time_features(df, time_local_col=time_local_col)
    op = _as_bool(dfx[operational_col])
    wk = _as_bool(dfx[workable_col])

    tmp_dict = {
        "year": dfx["year"],
        "month": dfx["month"],
        "_op": op,
        "_wk": wk,
    }
    if scenario_col in dfx.columns:
        tmp_dict[scenario_col] = dfx[scenario_col]

    tmp = pd.DataFrame(tmp_dict, index=dfx.index)

    group_cols = ["year", "month"]
    if scenario_col in tmp.columns:
        group_cols = [scenario_col, *group_cols]

    g = tmp.groupby(group_cols, as_index=False)
    out = g.agg(
        total_hours=("_op", "size"),
        operational_hours=("_op", "sum"),
        workable_hours=("_wk", "sum"),
    )

    out["schedule_lost_hours"] = out["total_hours"] - out["operational_hours"]
    out["weather_lost_hours"] = out["operational_hours"] - out["workable_hours"]

    # rates as fractions
    out["operational_pct_of_total_hours"] = out["operational_hours"] / out["total_hours"]
    out["workable_pct_of_total_hours"] = out["workable_hours"] / out["total_hours"]
    out["workable_pct_of_operational_hours"] = (
        out["workable_hours"] / out["operational_hours"].where(out["operational_hours"] != 0, pd.NA)
    ).fillna(0.0)
    out["weather_lost_pct_of_operational_hours"] = (
        out["weather_lost_hours"] / out["operational_hours"].where(out["operational_hours"] != 0, pd.NA)
    ).fillna(0.0)
    out["schedule_lost_pct_of_total_hours"] = out["schedule_lost_hours"] / out["total_hours"]

    sort_cols = ["year", "month"]
    if scenario_col in out.columns:
        sort_cols = [scenario_col, *sort_cols]
    out = out.sort_values(sort_cols).reset_index(drop=True)

    if drop_empty:
        out = out[(out["total_hours"] > 0)].reset_index(drop=True)

    return out


def summary_yearly(
    df: pd.DataFrame,
    *,
    time_local_col: str = "time_local",
    operational_col: str = "is_operational",
    workable_col: str = "is_workable",
    scenario_col: str = "scenario_id",
    drop_empty: bool = True,
) -> pd.DataFrame:
    """Yearly summary grouped by year."""
    dfx = extract_time_features(df, time_local_col=time_local_col)
    op = _as_bool(dfx[operational_col])
    wk = _as_bool(dfx[workable_col])

    tmp_dict = {"year": dfx["year"], "_op": op, "_wk": wk}
    if scenario_col in dfx.columns:
        tmp_dict[scenario_col] = dfx[scenario_col]
    tmp = pd.DataFrame(tmp_dict, index=dfx.index)

    group_cols = ["year"]
    if scenario_col in tmp.columns:
        group_cols = [scenario_col, *group_cols]
    g = tmp.groupby(group_cols, as_index=False)
    out = g.agg(
        total_hours=("_op", "size"),
        operational_hours=("_op", "sum"),
        workable_hours=("_wk", "sum"),
    )

    out["schedule_lost_hours"] = out["total_hours"] - out["operational_hours"]
    out["weather_lost_hours"] = out["operational_hours"] - out["workable_hours"]

    out["operational_pct_of_total_hours"] = out["operational_hours"] / out["total_hours"]
    out["workable_pct_of_total_hours"] = out["workable_hours"] / out["total_hours"]
    out["workable_pct_of_operational_hours"] = (
        out["workable_hours"] / out["operational_hours"].where(out["operational_hours"] != 0, pd.NA)
    ).fillna(0.0)
    out["weather_lost_pct_of_operational_hours"] = (
        out["weather_lost_hours"] / out["operational_hours"].where(out["operational_hours"] != 0, pd.NA)
    ).fillna(0.0)
    out["schedule_lost_pct_of_total_hours"] = out["schedule_lost_hours"] / out["total_hours"]

    sort_cols = ["year"]
    if scenario_col in out.columns:
        sort_cols = [scenario_col, *sort_cols]
    out = out.sort_values(sort_cols).reset_index(drop=True)
    if drop_empty:
        out = out[(out["total_hours"] > 0)].reset_index(drop=True)
    return out
