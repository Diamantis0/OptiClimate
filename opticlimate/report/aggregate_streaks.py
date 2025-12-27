from __future__ import annotations

import pandas as pd


def _infer_timestep_hours(ts: pd.Series) -> float:
    """Infer timestep in hours from timestamp series (robust to DST and duplicate timestamps)."""
    # IMPORTANT:
    # In multi-scenario tables, the same timestamp appears once per scenario.
    # If we don't drop duplicates, diffs will be 0 for those repeats and the
    # median timestep can become 0.0 -> all streak durations become 0.0.
    ts = ts.dropna().drop_duplicates().sort_values()
    diffs = ts.diff().dropna()
    if diffs.empty:
        return 1.0

    step = float(diffs.dt.total_seconds().median() / 3600.0)

    # Safety: avoid zero/negative/NaN timesteps (can happen with pathological inputs)
    if not pd.notna(step) or step <= 0:
        return 1.0

    return step


def _compute_streaks(
    df: pd.DataFrame,
    *,
    time_utc_col: str,
    value_col: str,
    scenario_col: str,
) -> pd.DataFrame:
    """
    Run-length encode boolean streaks. df must already be filtered to operational hours only.
    Breaks streaks when the boolean value flips OR scenario changes OR there is a time gap > 1.5 * timestep.
    """
    df = df.sort_values([scenario_col, time_utc_col]).copy()

    # Use duplicate-safe inference (critical for multi-scenario tables).
    timestep_hours = _infer_timestep_hours(df[time_utc_col])

    # Detect gaps (robust if there are missing hours)
    dt_hours = df.groupby(scenario_col)[time_utc_col].diff().dt.total_seconds() / 3600.0
    gap_break = dt_hours > (1.5 * timestep_hours)

    value_flip = df[value_col] != df[value_col].shift()
    scen_flip = df[scenario_col] != df[scenario_col].shift()

    boundary = value_flip | scen_flip | gap_break.fillna(False)

    df["_streak_id"] = boundary.cumsum()

    grouped = df.groupby([scenario_col, "_streak_id"], as_index=False)

    streaks = grouped.agg(
        start_ts=(time_utc_col, "first"),
        end_ts=(time_utc_col, "last"),
        value=(value_col, "first"),
        n_steps=(time_utc_col, "count"),
    )

    streaks = streaks.rename(columns={"_streak_id": "streak_id"})

    # With correct timestep_hours, a 1-hour streak has duration 1.0 (not 0.0).
    streaks["duration_hours"] = streaks["n_steps"] * float(timestep_hours)

    return streaks.drop(columns=["n_steps"])


def aggregate_streaks_operational(
    classified_df: pd.DataFrame,
    *,
    time_utc_col: str = "time_utc",
    operational_col: str = "operational_flag",
    workable_col: str = "workable_flag",
    scenario_col: str = "scenario_id",
) -> dict[str, pd.DataFrame]:
    """
    Compute workable / non-workable streaks during operational hours only.

    Returns:
      - streaks_nonworkable_operational
      - streaks_workable_operational
      - streaks_summary_operational
    """
    df = classified_df.copy()

    # Filter to operational time only
    if operational_col not in df.columns:
        raise KeyError(f"Expected column '{operational_col}' in classified_df")
    df = df[df[operational_col] == True]  # noqa: E712

    if df.empty:
        return {
            "streaks_nonworkable_operational": pd.DataFrame(),
            "streaks_workable_operational": pd.DataFrame(),
            "streaks_summary_operational": pd.DataFrame(),
        }

    if time_utc_col not in df.columns:
        raise KeyError(f"Expected column '{time_utc_col}' in classified_df")
    if workable_col not in df.columns:
        raise KeyError(f"Expected column '{workable_col}' in classified_df")
    if scenario_col not in df.columns:
        # baseline fallback for old runs
        df[scenario_col] = "baseline"

    # Build streak tables from workable_col
    all_streaks = _compute_streaks(
        df,
        time_utc_col=time_utc_col,
        value_col=workable_col,
        scenario_col=scenario_col,
    )

    blocked = all_streaks[all_streaks["value"] == False].drop(columns=["value"]).reset_index(drop=True)  # noqa: E712
    workable = all_streaks[all_streaks["value"] == True].drop(columns=["value"]).reset_index(drop=True)  # noqa: E712

    # Summary per scenario
    summary_rows: list[dict[str, float | str | int]] = []

    for scen in sorted(df[scenario_col].unique()):
        g = blocked[blocked[scenario_col] == scen]

        if g.empty:
            continue

        row = {
            "scenario_id": scen,
            "n_blocked_streaks": int(len(g)),
            "longest_blocked_hours": float(g["duration_hours"].max()),
            "p50_blocked_hours": float(g["duration_hours"].quantile(0.5)),
            "p90_blocked_hours": float(g["duration_hours"].quantile(0.9)),
            "count_blocked_ge_6h": int((g["duration_hours"] >= 6).sum()),
            "count_blocked_ge_12h": int((g["duration_hours"] >= 12).sum()),
            "count_blocked_ge_24h": int((g["duration_hours"] >= 24).sum()),
        }

        w = workable[workable[scenario_col] == scen]
        row.update(
            {
                "n_workable_streaks": int(len(w)),
                "longest_workable_hours": float(w["duration_hours"].max()) if not w.empty else 0.0,
                "p50_workable_hours": float(w["duration_hours"].quantile(0.5)) if not w.empty else 0.0,
                "p90_workable_hours": float(w["duration_hours"].quantile(0.9)) if not w.empty else 0.0,
            }
        )
        summary_rows.append(row)

    summary_df = pd.DataFrame(summary_rows)

    return {
        "streaks_nonworkable_operational": blocked,
        "streaks_workable_operational": workable,
        "streaks_summary_operational": summary_df,
    }
