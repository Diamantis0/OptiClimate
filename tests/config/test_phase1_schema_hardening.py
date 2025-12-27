import copy
import pytest

from opticlimate.config.normalize import normalize_config
from opticlimate.config.validate import validate_config, ConfigError


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
        "required_parameters": ["temperature"],
        "weather_thresholds": {"base": {"temperature": {"max": 40}}},
        "operational_window": {
            "time_bounds": {
                "start": "fixed_time",
                "start_time": "09:00",
                "end": "fixed_time",
                "end_time": "17:00",
            },
            "weekly_overrides": {},
        },
    }


def test_standard_3_plus_expands_with_custom_scenarios():
    raw = _base_cfg()
    raw["scenario_mode"] = "standard_3_plus"
    raw["custom_scenarios"] = ["stress_hot", "stress_windy"]

    cfg = normalize_config(copy.deepcopy(raw))
    validate_config(cfg)

    assert set(cfg["scenarios"]) == {
        "base", "conservative", "optimistic", "stress_hot", "stress_windy"
    }


def test_custom_scenarios_invalid_ids_rejected():
    raw = _base_cfg()
    raw["scenario_mode"] = "standard_3_plus"
    raw["custom_scenarios"] = ["bad id", "also/bad"]

    cfg = normalize_config(copy.deepcopy(raw))
    with pytest.raises(ConfigError, match=r"Invalid scenario id"):
        validate_config(cfg)


def test_operational_window_flat_keys_rejected():
    raw = _base_cfg()
    raw["operational_window"] = {
        "calendar_model": "all_days",
        "daylight_model": "none",
        # legacy flat keys (should be rejected)
        "start": "fixed_time",
        "start_time": "09:00",
        "end": "fixed_time",
        "end_time": "17:00",
    }

    cfg = normalize_config(copy.deepcopy(raw))
    with pytest.raises(ConfigError, match=r"Legacy operational_window keys"):
        validate_config(cfg)
