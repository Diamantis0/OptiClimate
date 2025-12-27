# tests/helpers.py

import copy

from opticlimate.config.normalize import normalize_config
from opticlimate.config.validate import validate_config


def base_cfg():
    """Return a strict-schema config baseline that passes validation."""
    return {
        "project": {
            "id": "demo",
            "name": "Demo",
            "activity_type": "",
            "units": "metric",
            "granularity": "hourly",
            "location": {
                "latitude": 40.0,
                "longitude": -105.0,
                "timezone": "America/Denver",
                "elevation": None,
            },
            "analysis_period": {
                "period_start": "01-01",
                "period_end": "12-31",
                "analysis_end_year": 2024,
                "historic_years": 2,
            },
        },
        "required_parameters": ["temperature"],
        "scenario_mode": "base_only",
        "custom_scenarios": [],
        "weather_thresholds": {"base": {}},
        "operational_window": {
            "calendar_model": "all_days",
            "daylight_model": "none",
            "time_bounds": {
                "start": "fixed_time",
                "end": "fixed_time",
                "start_time": "09:00",
                "end_time": "17:00",
            },
            "weekly_overrides": {},
        },
    }


def norm_and_validate(cfg: dict) -> dict:
    cfg2 = normalize_config(copy.deepcopy(cfg))
    validate_config(cfg2)
    return cfg2
