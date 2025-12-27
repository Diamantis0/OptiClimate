# opticlimate/temporal/operational_window.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Set, Tuple

import pandas as pd


@dataclass(frozen=True)
class OperationalMaskResult:
    is_operational: pd.Series  # bool Series aligned to df index
    reason: str


def _parse_hhmm(s: str) -> Tuple[int, int]:
    hh = int(s[:2])
    mm = int(s[3:])
    return hh, mm


def _weekdays_set(ow: Dict[str, Any]) -> Set[int]:
    cal = ow.get("calendar_model")
    if cal == "custom":
        w = ow.get("weekdays", [])
        return {int(x) for x in w}
    return set(range(7))  # all days


def build_operational_mask_fixed_time(
    df: pd.DataFrame,
    ow: Dict[str, Any],
    time_col: str = "time_local",
) -> OperationalMaskResult:
    """
    Build an operational mask for FIXED TIME windows only (current phase).

    Supports:
      - calendar_model: all_days OR custom (weekdays)
      - time_bounds: start=fixed_time AND end=fixed_time
      - weekly_overrides: optional per weekday overrides (fixed_time only)

    Returns a boolean Series aligned to df.index.
    """
    if time_col not in df.columns:
        raise ValueError(f"DataFrame missing required time column {time_col!r}")

    t = df[time_col]
    if not isinstance(t.dtype, pd.DatetimeTZDtype):
        raise ValueError(f"{time_col!r} must be timezone-aware datetimes")

    ow = ow or {}
    tb = ow.get("time_bounds") or {}

    # Enforce: fixed_time -> fixed_time only
    if tb.get("start") != "fixed_time" or tb.get("end") != "fixed_time":
        raise NotImplementedError(
            "This phase supports only fixed_time -> fixed_time time_bounds for operational masking"
        )

    start_time = tb.get("start_time")
    end_time = tb.get("end_time")
    if not start_time or not end_time:
        raise ValueError("fixed_time -> fixed_time requires start_time and end_time")

    sh, sm = _parse_hhmm(start_time)
    eh, em = _parse_hhmm(end_time)

    allowed_weekdays = _weekdays_set(ow)

    # Base weekday filter (local time)
    weekday_ok = t.dt.weekday.isin(sorted(allowed_weekdays))

    # Base time-of-day filter: [start, end) in local time
    minutes = t.dt.hour * 60 + t.dt.minute
    start_min = sh * 60 + sm
    end_min = eh * 60 + em
    if end_min <= start_min:
        raise ValueError("fixed_time window must have end_time after start_time (same-day window)")

    time_ok = (minutes >= start_min) & (minutes < end_min)
    base_mask = (weekday_ok & time_ok).astype(bool)

    # Weekly overrides (optional): fixed_time only
    weekly = ow.get("weekly_overrides") or {}
    if not isinstance(weekly, dict):
        raise ValueError("weekly_overrides must be a dict if provided")

    if not weekly:
        return OperationalMaskResult(is_operational=base_mask, reason="fixed_time")

    mask = base_mask.copy()

    for day_key, override in weekly.items():
        d = int(day_key)
        if d < 0 or d > 6:
            raise ValueError("weekly_overrides keys must be 0..6")
        if not isinstance(override, dict):
            raise ValueError(f"weekly_overrides[{day_key}] must be a dict")

        os = override.get("start", tb.get("start"))
        oe = override.get("end", tb.get("end"))
        if os != "fixed_time" or oe != "fixed_time":
            raise NotImplementedError("weekly_overrides supports only fixed_time -> fixed_time in this phase")

        ost = override.get("start_time", start_time)
        oet = override.get("end_time", end_time)
        osh, osm = _parse_hhmm(ost)
        oeh, oem = _parse_hhmm(oet)

        ostart_min = osh * 60 + osm
        oend_min = oeh * 60 + oem
        if oend_min <= ostart_min:
            raise ValueError(f"weekly_overrides[{day_key}] end_time must be after start_time")

        # apply override only to rows of that weekday
        is_day = (t.dt.weekday == d)
        o_time_ok = (minutes >= ostart_min) & (minutes < oend_min)

        # If custom weekdays excludes this day, it stays excluded
        mask.loc[is_day] = weekday_ok.loc[is_day] & o_time_ok.loc[is_day]

    return OperationalMaskResult(is_operational=mask.astype(bool), reason="fixed_time_with_overrides")
