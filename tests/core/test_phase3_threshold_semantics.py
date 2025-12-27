from __future__ import annotations

from datetime import datetime, timedelta, timezone
import copy

import numpy as np
import pandas as pd

from opticlimate.classify import classify_baseline
from opticlimate.config.normalize import normalize_config
from opticlimate.config.validate import validate_config


def _cfg_with_thresholds(thresholds: dict, required: list[str]):
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
        "scenario_mode": "base_only",
        "custom_scenarios": [],
        "required_parameters": required,
        "weather_thresholds": {"base": thresholds},
        "operational_window": {
            "calendar_model": "all_days",
            "daylight_model": "none",
            "time_bounds": {
                "start": "fixed_time",
                "start_time": "00:00",
                "end": "fixed_time",
                "end_time": "23:59",
            },
            "weekly_overrides": {},
        },
    }


def _truth_df(rows: list[dict]) -> pd.DataFrame:
    t0 = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    times = [t0 + timedelta(hours=i) for i in range(len(rows))]
    df = pd.DataFrame(rows)
    df.insert(0, "time_local", pd.to_datetime(times, utc=True))
    df.insert(0, "time_utc", pd.to_datetime(times, utc=True))
    return df


def _classify(cfg: dict, df: pd.DataFrame) -> pd.DataFrame:
    cfg2 = normalize_config(copy.deepcopy(cfg))
    validate_config(cfg2)
    return classify_baseline(df, cfg2, scenario_id="base").classified_df


def test_threshold_bounds_are_inclusive():
    # min/max should include the boundary values (>=min and <=max)
    cfg = _cfg_with_thresholds(
        thresholds={"temperature": {"min": 0.0, "max": 40.0}},
        required=["temperature"],
    )

    df = _truth_df(
        [
            {"temperature": 0.0},    # exactly min -> workable
            {"temperature": 40.0},   # exactly max -> workable
            {"temperature": -0.1},   # below min -> blocked
            {"temperature": 40.1},   # above max -> blocked
        ]
    )
    out = _classify(cfg, df)

    assert out["operational_flag"].all()
    assert out["workable_flag"].tolist() == [True, True, False, False]


def test_nan_blocks_when_threshold_present():
    cfg = _cfg_with_thresholds(
        thresholds={"temperature": {"min": 0.0}},
        required=["temperature"],
    )

    df = _truth_df(
        [
            {"temperature": np.nan},  # threshold exists => NaN should block
            {"temperature": 10.0},
        ]
    )
    out = _classify(cfg, df)

    assert out["workable_flag"].tolist() == [False, True]


def test_limiting_param_is_deterministic_with_multiple_failures():
    # both fail on row 0, only wind fails on row 1
    cfg = _cfg_with_thresholds(
        thresholds={
            "temperature": {"max": 0.0},
            "wind_speed": {"max": 5.0},
        },
        required=["temperature", "wind_speed"],
    )

    df = _truth_df(
        [
            {"temperature": 10.0, "wind_speed": 20.0},  # both fail
            {"temperature": 0.0, "wind_speed": 20.0},   # wind fails only
        ]
    )

    out = _classify(cfg, df)

    assert out["workable_flag"].tolist() == [False, False]
    # second row must clearly be wind_speed
    assert out.loc[1, "limiting_param"] == "wind_speed"
    # first row: whichever your implementation chooses, it must be consistent.
    # We enforce that it must be one of the failed params (not None/other).
    assert out.loc[0, "limiting_param"] in {"temperature", "wind_speed"}
