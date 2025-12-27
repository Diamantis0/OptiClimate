# opticlimate/pipeline/run.py

from __future__ import annotations

import argparse
from typing import Any, Dict, Tuple

import pandas as pd

from opticlimate.classify import classify_baseline
from opticlimate.config.normalize import normalize_config
from opticlimate.config.validate import validate_config
from opticlimate.fetch.open_meteo import fetch_hourly_weather
from opticlimate.report.build import build_core_bundle
from opticlimate.utils.io import load_config_file


def _date_range_from_cfg(cfg: Dict[str, Any]) -> Tuple[str, str]:
    """
    Fetch full years spanning analysis_years.
    Example: [2020..2024] -> 2020-01-01 .. 2024-12-31
    """
    ap = cfg["project"]["analysis_period"]
    years = ap["analysis_years"]
    y0, y1 = int(years[0]), int(years[-1])
    return f"{y0:04d}-01-01", f"{y1:04d}-12-31"


def run_pipeline(config_path: str) -> int:
    raw = load_config_file(config_path)
    cfg = normalize_config(raw)
    validate_config(cfg)

    loc = cfg["project"]["location"]
    start_date, end_date = _date_range_from_cfg(cfg)
    params = cfg["required_parameters"]

    truth_df = fetch_hourly_weather(
        latitude=float(loc["latitude"]),
        longitude=float(loc["longitude"]),
        timezone=str(loc["timezone"]),
        start_date=start_date,
        end_date=end_date,
        parameters=params,
    )

    print(f"OK fetched rows={len(truth_df)} cols={list(truth_df.columns)}")

    # Phase 2: classification -> workable (scenario-aware)
    # Build a classified dataframe per scenario and concatenate.
    scenarios = [str(s) for s in cfg["scenarios"]]
    classified_parts: list[pd.DataFrame] = []

    for scen in scenarios:
        result = classify_baseline(truth_df, cfg, scenario_id=str(scen))
        classified_parts.append(result.classified_df)

    classified_df = pd.concat(classified_parts, axis=0, ignore_index=True)

    # Reporting (schema-driven)
    bundle = build_core_bundle(
        classified_df,
        cfg=cfg,
        include_weather_stats=False,  # set True when you want 5C stats in pipeline
        weather_params=params,        # explicit columns expected for stats (if enabled)
        validate=True,
    )

    # --- Phase 3a stats preview ---
    dist = bundle.tables.get("stats_monthly_workable_rate_dist")
    if dist is not None:
        print("\nSTATS: MONTHLY WORKABLE RATE DISTRIBUTION:")
        print(dist.to_string(index=False))

    targets = bundle.tables.get("reliability_targets_monthly")
    if targets is not None:
        print("\nSTATS: RELIABILITY TARGETS (MONTHLY):")
        print(targets.to_string(index=False))


    streaks = bundle.tables.get("streaks_summary_operational")
    if streaks is not None and not streaks.empty:
        print("\nSTREAKS SUMMARY (OPERATIONAL):")
        print(streaks.to_string(index=False))

    blocked = bundle.tables.get("streaks_nonworkable_operational")
    if blocked is not None and not blocked.empty:
        print("\nTOP 10 LONGEST BLOCKED STREAKS:")
        print(blocked.sort_values("duration_hours", ascending=False).head(10).to_string(index=False))


    # Print per-scenario run summary (consistent denominators)
    for _, rs in bundle.tables["summary_run"].iterrows():
        scen = rs.get("scenario_id", "baseline")

        total_hours = float(rs["total_hours"])
        operational_hours = float(rs["operational_hours"])
        workable_hours = float(rs["workable_hours"])
        schedule_lost_hours = float(rs["schedule_lost_hours"])
        weather_lost_hours = float(rs["weather_lost_hours"])

        workable_pct_op = 100.0 * (workable_hours / operational_hours) if operational_hours else 0.0
        weather_lost_pct_op = 100.0 * (weather_lost_hours / operational_hours) if operational_hours else 0.0
        schedule_lost_pct_total = 100.0 * (schedule_lost_hours / total_hours) if total_hours else 0.0
        operational_pct_total = 100.0 * (operational_hours / total_hours) if total_hours else 0.0

        print(
            f"RUN SUMMARY [{scen}]: workable={int(workable_hours)}/{int(operational_hours)} "
            f"({workable_pct_op:.1f}% of operational)"
        )
        print(
            f"LOST HOURS [{scen}]: "
            f"schedule={int(schedule_lost_hours)}h ({schedule_lost_pct_total:.1f}% of total), "
            f"weather={int(weather_lost_hours)}h ({weather_lost_pct_op:.1f}% of operational), "
            f"operational={int(operational_hours)}h ({operational_pct_total:.1f}% of total)"
        )


    # Monthly / yearly summaries from bundle (schema-driven)
    m = bundle.tables["summary_monthly"]
    y = bundle.tables["summary_yearly"]

    print("\nMONTHLY SUMMARY (first 6 rows):")
    print(m.head(6).to_string(index=False))

    print("\nYEARLY SUMMARY:")
    print(y.to_string(index=False))

    # Limiting factors (loss by param)
    lf = bundle.tables.get("loss_by_param_monthly")
    if lf is not None and not lf.empty:
        print("\nLIMITING FACTORS (monthly top 10 rows):")
        print(lf.head(10).to_string(index=False))
    else:
        print("\nLIMITING FACTORS: (no blocked operational hours with identified limiting parameter)")

    print("\nOperational window mode: reason=fixed_time")

    # Hourly preview (classified rows include operational/workable + limiting_param)
    print("\nHOURLY PREVIEW:")
    print(classified_df.head(5).to_string(index=False))

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="opticlimate", description="OptiClimate V2 pipeline runner")
    parser.add_argument("config", help="Path to config (.yml/.yaml/.json)")
    args = parser.parse_args(argv)
    return run_pipeline(args.config)
