import copy
from datetime import datetime, timedelta, timezone

import pandas as pd

from opticlimate.classify import classify_baseline
from opticlimate.config.normalize import normalize_config
from opticlimate.config.validate import validate_config
from opticlimate.report.build import build_core_bundle

from tests.helpers import base_cfg


def test_thresholds_applied_per_scenario_distinct_results():
    cfg = base_cfg()
    cfg["scenario_mode"] = "standard_3"
    cfg["custom_scenarios"] = []

    cfg["required_parameters"] = ["temperature", "wind_speed", "precipitation"]

    # Scenario-first thresholds. Wind differs across scenarios.
    cfg["weather_thresholds"] = {
        "base": {
            "wind_speed": {"max": 20},
            "temperature": {"min": -50, "max": 60},
            "precipitation": {"max": 999},
        },
        "conservative": {
            "wind_speed": {"max": 10},
            "temperature": {"min": -50, "max": 60},
            "precipitation": {"max": 999},
        },
        "optimistic": {
            "wind_speed": {"max": 30},
            "temperature": {"min": -50, "max": 60},
            "precipitation": {"max": 999},
        },
    }

    cfg2 = normalize_config(copy.deepcopy(cfg))
    validate_config(cfg2)

    # 24h synthetic day (UTC). Operational window comes from cfg.
    start_utc = datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc)
    times_utc = [start_utc + timedelta(hours=h) for h in range(24)]
    time_utc = pd.to_datetime(times_utc, utc=True)
    time_local = time_utc.tz_convert("America/Denver")

    truth_df = pd.DataFrame(
        {
            "time_utc": time_utc,
            "time_local": time_local,
            "temperature": [10.0] * 24,
            "wind_speed": [15.0] * 24,
            "precipitation": [0.0] * 24,
        }
    )

    classified_parts = []
    for scen in cfg2["scenarios"]:
        res = classify_baseline(truth_df, cfg2, scenario_id=str(scen))
        classified_parts.append(res.classified_df)

    classified_df = pd.concat(classified_parts, ignore_index=True)

    bundle = build_core_bundle(
        classified_df,
        cfg=cfg2,
        include_weather_stats=False,
        weather_params=cfg2["required_parameters"],
        validate=True,
    )

    sr = bundle.tables["summary_run"].set_index("scenario_id")

    # wind_speed=15 during operational hours:
    # conservative (max 10) blocks; base (max 20) passes; optimistic (max 30) passes.
    assert int(sr.loc["base", "workable_hours"]) == int(sr.loc["base", "operational_hours"])
    assert int(sr.loc["optimistic", "workable_hours"]) == int(sr.loc["optimistic", "operational_hours"])
    assert int(sr.loc["conservative", "workable_hours"]) == 0
    assert int(sr.loc["conservative", "operational_hours"]) > 0
