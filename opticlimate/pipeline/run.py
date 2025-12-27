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
from opticlimate.report.export import export_bundle
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
    run_id_raw = str(cfg.get("run_id", ""))
    bundle = build_core_bundle(
        classified_df,
        cfg=cfg,
        run_id=run_id_raw,
        include_weather_stats=False,  # set True when you want 5C stats in pipeline
        weather_params=params,        # explicit columns expected for stats (if enabled)
        validate=True,
    )

    out_dir = export_bundle(bundle, run_id=run_id_raw, out_root="outputs", overwrite=True, formats=("parquet", "csv"))
    print(f"Wrote outputs to {out_dir}")

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="opticlimate", description="OptiClimate V2 pipeline runner")
    parser.add_argument("config", help="Path to config (.yml/.yaml/.json)")
    args = parser.parse_args(argv)
    return run_pipeline(args.config)
