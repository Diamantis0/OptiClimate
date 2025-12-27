import copy
from datetime import datetime, timedelta, timezone

import pandas as pd

from opticlimate.classify import classify_baseline
from opticlimate.config.normalize import normalize_config
from opticlimate.config.validate import validate_config
from opticlimate.report.build import build_core_bundle

from tests.helpers import base_cfg


def test_summary_accounting_identities_hold():
    cfg = base_cfg()
    cfg["scenario_mode"] = "standard_3"
    cfg["custom_scenarios"] = []
    cfg["required_parameters"] = ["temperature"]
    cfg["weather_thresholds"] = {
        "base": {"temperature": {"min": -50, "max": 60}},
        "conservative": {"temperature": {"min": -20, "max": 50}},
        "optimistic": {"temperature": {"min": -60, "max": 70}},
    }

    cfg2 = normalize_config(copy.deepcopy(cfg))
    validate_config(cfg2)

    start_utc = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    n = 24 * 3
    times_utc = [start_utc + timedelta(hours=h) for h in range(n)]
    time_utc = pd.to_datetime(times_utc, utc=True)
    time_local = time_utc.tz_convert("America/Denver")

    truth_df = pd.DataFrame(
        {
            "time_utc": time_utc,
            "time_local": time_local,
            "temperature": [10.0] * n,
        }
    )

    classified_df = pd.concat(
        [classify_baseline(truth_df, cfg2, scenario_id=str(s)).classified_df for s in cfg2["scenarios"]],
        ignore_index=True,
    )

    bundle = build_core_bundle(
        classified_df,
        cfg=cfg2,
        include_weather_stats=False,
        weather_params=cfg2["required_parameters"],
        validate=True,
    )

    for table_name in ("summary_monthly", "summary_yearly"):
        df = bundle.tables[table_name]
        assert not df.empty
        for _, r in df.iterrows():
            workable = float(r["workable_hours"])
            weather_lost = float(r["weather_lost_hours"])
            operational = float(r["operational_hours"])
            schedule_lost = float(r["schedule_lost_hours"])
            total = float(r["total_hours"])

            assert abs((workable + weather_lost) - operational) < 1e-6
            assert abs((schedule_lost + operational) - total) < 1e-6
