from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone

import pandas as pd

from opticlimate.classify import classify_baseline
from opticlimate.config.normalize import normalize_config
from opticlimate.config.validate import validate_config
from opticlimate.report.build import build_core_bundle


def test_end_to_end_smoke_offline_multiscenario():
    raw_cfg = {
        "project": {
            "id": "demo",
            "name": "Demo",
            "activity_type": "",
            "units": "metric",
            "granularity": "hourly",
            "location": {"latitude": 39.7392, "longitude": -104.9903, "timezone": "UTC"},
            "analysis_period": {"analysis_end_year": 2024, "historic_years": 1},
        },
        "scenario_mode": "standard_3",
        "required_parameters": ["temperature", "wind_speed", "precipitation"],
        "weather_thresholds": {
            "base": {"temperature": {"max": 40}, "wind_speed": {"max": 20}, "precipitation": {"max": 2.0}},
            "conservative": {"temperature": {"max": 35}, "wind_speed": {"max": 15}, "precipitation": {"max": 1.0}},
            "optimistic": {"temperature": {"max": 45}, "wind_speed": {"max": 25}, "precipitation": {"max": 3.0}},
        },
        "operational_window": {
            "calendar_model": "all_days",
            "daylight_model": "none",
            "time_bounds": {"start": "fixed_time", "start_time": "09:00", "end": "fixed_time", "end_time": "17:00"},
            "weekly_overrides": {},
        },
    }

    cfg = normalize_config(copy.deepcopy(raw_cfg))
    validate_config(cfg)

    # 14 days hourly: enough to produce monthly/yearly groupings safely in most code paths
    t0 = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    times = [t0 + timedelta(hours=i) for i in range(24 * 14)]
    t = pd.to_datetime(times, utc=True)

    # Synthetic weather that will cause conservative to lose more hours than optimistic
    truth = pd.DataFrame(
        {
            "time_utc": t,
            "time_local": t,
            "temperature": [38.0] * len(t),      # blocks conservative max 35
            "wind_speed": [18.0] * len(t),       # ok for base/opt, borderline for conservative max 15
            "precipitation": [1.5] * len(t),     # ok for base/opt, blocks conservative max 1.0
        }
    )

    classified_parts = []
    for s in cfg["scenarios"]:
        classified_parts.append(classify_baseline(truth, cfg, scenario_id=s).classified_df)
    classified = pd.concat(classified_parts, ignore_index=True)

    bundle = build_core_bundle(
        classified,
        cfg=cfg,
        include_weather_stats=False,
        weather_params=cfg["required_parameters"],
        validate=True,
    )

    # Core tables must exist and be non-empty
    for key in ("summary_run", "summary_monthly", "summary_yearly"):
        assert key in bundle.tables
        assert not bundle.tables[key].empty

    # Accounting identity holds for every scenario in summary_run
    sr = bundle.tables["summary_run"]
    assert (sr["workable_hours"] + sr["weather_lost_hours"] == sr["operational_hours"]).all()
    assert (sr["operational_hours"] + sr["schedule_lost_hours"] == sr["total_hours"]).all()

    # Sanity ordering: optimistic should be >= base >= conservative in workable hours
    sr2 = sr.set_index("scenario_id")
    assert sr2.loc["optimistic", "workable_hours"] >= sr2.loc["base", "workable_hours"]
    assert sr2.loc["base", "workable_hours"] >= sr2.loc["conservative", "workable_hours"]
