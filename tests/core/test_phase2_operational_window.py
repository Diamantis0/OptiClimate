from __future__ import annotations

from datetime import datetime, timedelta, timezone
import copy

import pandas as pd
import pytest   # ← ADD THIS

from opticlimate.classify import classify_baseline
from opticlimate.config.normalize import normalize_config
from opticlimate.config.validate import validate_config
from opticlimate.report.build import build_core_bundle



def _base_cfg():
    return {
        "project": {
            "id": "demo",
            "name": "Demo",
            "activity_type": "",
            "units": "metric",
            "granularity": "hourly",
            "location": {"latitude": 0, "longitude": 0, "timezone": "UTC"},
            "analysis_period": {"analysis_end_year": 2024, "historic_years": 1},
        },
        "scenario_mode": "base_only",
        "custom_scenarios": [],
        "required_parameters": ["temperature"],
        "weather_thresholds": {"base": {"temperature": {"min": -100, "max": 100}}},
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


def _truth_df(hours: int, start_utc: datetime) -> pd.DataFrame:
    times = [start_utc + timedelta(hours=h) for h in range(hours)]
    time_utc = pd.to_datetime(times, utc=True)
    return pd.DataFrame(
        {
            "time_utc": time_utc,
            "time_local": time_utc,  # UTC tz, OK for deterministic tests
            "temperature": [10.0] * hours,
        }
    )


def _run_summary(cfg: dict, truth_df: pd.DataFrame) -> pd.Series:
    cfg2 = normalize_config(copy.deepcopy(cfg))
    validate_config(cfg2)

    scen = cfg2["scenarios"][0]
    classified_df = classify_baseline(truth_df, cfg2, scenario_id=scen).classified_df

    bundle = build_core_bundle(
        classified_df,
        cfg=cfg2,
        include_weather_stats=False,
        weather_params=cfg2["required_parameters"],
        validate=True,
    )
    # summary_run is per-scenario; base_only so take first row
    return bundle.tables["summary_run"].iloc[0]


def test_cross_midnight_window_is_rejected_in_current_phase():
    cfg = _base_cfg()
    cfg["operational_window"]["time_bounds"] = {
        "start": "fixed_time",
        "start_time": "22:00",
        "end": "fixed_time",
        "end_time": "06:00",
    }

    df = _truth_df(48, datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc))

    with pytest.raises(ValueError, match="same-day window"):
        _run_summary(cfg, df)


def test_weekly_override_can_shorten_a_day():
    cfg = _base_cfg()
    # baseline window 09-17 => 8h/day for all days => 56h/week
    # shorten Friday (weekday 4) to 09-12 => 3h on Friday (reduce by 5h)
    cfg["operational_window"]["weekly_overrides"] = {
        "4": {
            "start": "fixed_time",
            "start_time": "09:00",
            "end": "fixed_time",
            "end_time": "12:00",
        }
    }

    df = _truth_df(24 * 7, datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc))
    sr = _run_summary(cfg, df)

    assert int(sr["total_hours"]) == 168
    assert int(sr["operational_hours"]) == 51  # 56 - 5
    assert int(sr["workable_hours"]) == 51
