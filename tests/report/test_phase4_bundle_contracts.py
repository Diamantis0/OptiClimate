from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone

import pandas as pd

from opticlimate.classify import classify_baseline
from opticlimate.config.normalize import normalize_config
from opticlimate.config.validate import validate_config
from opticlimate.report.build import build_core_bundle


def _cfg_standard_3():
    return {
        "run_id": "demo_project",
        "project": {
            "id": "demo",
            "name": "Demo",
            "activity_type": "",
            "units": "metric",
            "granularity": "hourly",
            "location": {"latitude": 0, "longitude": 0, "timezone": "UTC"},
            "analysis_period": {"analysis_end_year": 2024, "historic_years": 1},
        },
        "scenario_mode": "standard_3",
        "custom_scenarios": [],
        "required_parameters": ["temperature", "wind_speed"],
        "weather_thresholds": {
            "base": {"temperature": {"max": 40}, "wind_speed": {"max": 20}},
            "conservative": {"temperature": {"max": 30}, "wind_speed": {"max": 15}},
            "optimistic": {"temperature": {"max": 45}, "wind_speed": {"max": 25}},
        },
        "operational_window": {
            "calendar_model": "all_days",
            "daylight_model": "none",
            "time_bounds": {
                "start": "fixed_time",
                "start_time": "09:00",
                "end": "fixed_time",
                "end_time": "17:00",
            },
            "weekly_overrides": {},
        },
    }


def _truth_df(hours: int = 72) -> pd.DataFrame:
    # 3 days hourly
    t0 = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    times = [t0 + timedelta(hours=i) for i in range(hours)]
    time_utc = pd.to_datetime(times, utc=True)
    # pick values that cause some failures in conservative but not optimistic
    return pd.DataFrame(
        {
            "time_utc": time_utc,
            "time_local": time_utc,
            "temperature": [35.0] * hours,
            "wind_speed": [18.0] * hours,
        }
    )


def _bundle(cfg: dict, df: pd.DataFrame):
    cfg2 = normalize_config(copy.deepcopy(cfg))
    validate_config(cfg2)

    parts = []
    for s in cfg2["scenarios"]:
        parts.append(classify_baseline(df, cfg2, scenario_id=s).classified_df)
    classified = pd.concat(parts, ignore_index=True)

    return cfg2, build_core_bundle(
        classified,
        cfg=cfg2,
        include_weather_stats=False,
        weather_params=cfg2["required_parameters"],
        validate=True,
    )


def test_bundle_tables_cover_all_scenarios():
    cfg, bundle = _bundle(_cfg_standard_3(), _truth_df())
    expected = set(cfg["scenarios"])

    # Tables that must be per-scenario
    must_cover = [
        "summary_run",
        "summary_monthly",
        "summary_yearly",
        "reliability_targets_monthly",
        "stats_monthly_workable_rate_dist",
    ]

    for name in must_cover:
        t = bundle.tables.get(name)
        assert t is not None and not t.empty, f"Missing/empty table: {name}"
        assert set(t["scenario_id"].unique()) == expected, f"{name} missing scenarios"


def test_summary_run_has_required_columns():
    _, bundle = _bundle(_cfg_standard_3(), _truth_df())
    t = bundle.tables["summary_run"]

    required_cols = {
        "scenario_id",
        "total_hours",
        "operational_hours",
        "workable_hours",
        "schedule_lost_hours",
        "weather_lost_hours",
        "workable_pct_of_operational_hours",
        "weather_lost_pct_of_operational_hours",
        "operational_pct_of_total_hours",
    }
    assert required_cols.issubset(set(t.columns))
