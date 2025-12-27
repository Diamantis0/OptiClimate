from __future__ import annotations

import json
from pathlib import Path

import pytest

from opticlimate.pipeline.run import run_pipeline


@pytest.mark.manual
def test_openmeteo_pipeline_smoke(tmp_path: Path):
    """
    Manual integration smoke test:
    - Calls Open-Meteo via the real pipeline runner
    - Ensures the pipeline completes end-to-end without exceptions
    - Uses a small scenario set (base_only) to keep it lighter

    Note: Pipeline currently fetches whole years based on analysis_years,
    so this will still pull a full year of hourly data. That's OK for manual runs.
    """
    cfg = {
        "project": {
            "id": "manual-smoke",
            "name": "Manual Smoke Test",
            "activity_type": "",
            "units": "metric",
            "granularity": "hourly",
            # Denver-ish; any valid lat/lon is fine
            "location": {"latitude": 39.7392, "longitude": -104.9903, "timezone": "UTC"},
            "analysis_period": {"analysis_end_year": 2024, "historic_years": 1},
        },
        "scenario_mode": "base_only",
        "custom_scenarios": [],
        "required_parameters": ["temperature", "wind_speed", "precipitation"],
        "weather_thresholds": {
            "base": {
                "temperature": {"min": -30, "max": 45},
                "wind_speed": {"max": 25},
                "precipitation": {"max": 3.0},
            }
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

    config_path = tmp_path / "manual_smoke_config.json"
    config_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")

    rc = run_pipeline(str(config_path))
    assert rc == 0
