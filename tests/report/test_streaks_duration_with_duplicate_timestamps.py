from datetime import datetime, timezone

import pandas as pd

from opticlimate.report.aggregate_streaks import aggregate_streaks_operational


def test_streaks_duration_nonzero_with_duplicate_timestamps():
    # Two scenarios share the same timestamps; regression: timestep inference must not return 0.
    t0 = pd.to_datetime(datetime(2024, 1, 1, 16, 0, tzinfo=timezone.utc))
    t1 = pd.to_datetime(datetime(2024, 1, 1, 17, 0, tzinfo=timezone.utc))

    df = pd.DataFrame(
        {
            "time_utc": [t0, t1, t0, t1],
            "scenario_id": ["base", "base", "optimistic", "optimistic"],
            "operational_flag": [True, True, True, True],
            "workable_flag": [False, False, False, False],
        }
    )

    out = aggregate_streaks_operational(df)
    blocked = out["streaks_nonworkable_operational"]
    assert not blocked.empty

    # One blocked streak per scenario, each lasting 2 hours (2 steps * 1 hour timestep)
    dur_by_scen = blocked.groupby("scenario_id")["duration_hours"].max().to_dict()
    assert dur_by_scen["base"] == 2.0
    assert dur_by_scen["optimistic"] == 2.0
