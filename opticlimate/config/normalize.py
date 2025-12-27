# opticlimate/config/normalize.py

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

from opticlimate.config.scenarios import expand_scenarios
from opticlimate.config.thresholds import normalize_weather_thresholds


def _as_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def normalize_config(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize user-provided config into a canonical internal structure.

    This function:
      - fills defaults
      - coerces types where safe
      - derives analysis_years from (analysis_end_year, historic_years)
      - does NOT validate constraints (that's validate.py)

    Canonical minimal output fields created/ensured:
      project.{id,name,activity_type,units,granularity,location,analysis_period}
      required_parameters (list)
      scenarios (list)
      weather_thresholds (dict)
      operational_window (dict)
    """
    cfg: Dict[str, Any] = deepcopy(raw) if isinstance(raw, dict) else {}

    # -------------------------
    # Strict-mode legacy input detection
    # -------------------------
    legacy: Dict[str, bool] = {}
    if isinstance(raw, dict):
        # `scenarios:` is legacy / disallowed (must use scenario_mode + custom_scenarios).
        if 'scenarios' in raw:
            legacy['scenarios_key'] = True
        # Legacy operational_window flat keys are disallowed (must use operational_window.time_bounds).
        ow_raw = raw.get('operational_window')
        if isinstance(ow_raw, dict) and any(k in ow_raw for k in ('start', 'end', 'start_time', 'end_time')):
            # time_bounds is allowed; flat siblings are not
            if 'time_bounds' not in ow_raw:
                legacy['operational_window_flat'] = True
            else:
                # if time_bounds exists but flat keys also present, still flag
                flat = {k for k in ('start','end','start_time','end_time') if k in ow_raw}
                if flat:
                    legacy['operational_window_flat'] = True
        # Legacy param-first weather_thresholds is disallowed.
        wt_raw = raw.get('weather_thresholds')
        if isinstance(wt_raw, dict):
            # Heuristic: if any top-level key looks like a weather parameter (contains underscore or common names),
            # validator will decide; we just flag obvious param-first patterns via nested scenario dicts.
            from opticlimate.config.schema import SUPPORTED_WEATHER_PARAMETERS
            if any(k in SUPPORTED_WEATHER_PARAMETERS for k in wt_raw.keys()):
                legacy['weather_thresholds_param_first'] = True

    if legacy:
        cfg['_legacy_inputs'] = legacy

    # -------------------------
    # Top-level defaults
    # -------------------------
    cfg.setdefault("run_id", "")
    if isinstance(cfg.get("run_id"), str):
        cfg["run_id"] = cfg["run_id"].strip()
    elif cfg.get("run_id") is not None:
        # Best-effort coerce to string; validation will enforce non-empty.
        cfg["run_id"] = str(cfg["run_id"]).strip()

    cfg.setdefault("required_parameters", [])
    cfg.setdefault("scenarios", [])
    # new scenario-set mode inputs (optional)
    cfg.setdefault("scenario_mode", "base_only")
    cfg.setdefault("custom_scenarios", [])

    cfg.setdefault("weather_thresholds", {})
    cfg.setdefault("operational_window", {})

    # -------------------------
    # Project defaults
    # -------------------------
    project = cfg.get("project")
    if not isinstance(project, dict):
        project = {}
        cfg["project"] = project
    project.setdefault("id", "")
    project.setdefault("name", "")
    project.setdefault("activity_type", "")
    project.setdefault("units", "metric")
    project.setdefault("granularity", "hourly")

    # -------------------------
    # Location defaults
    # -------------------------
    loc = project.setdefault("location", {})
    # do NOT default timezone; missing timezone should be a validation error
    loc.setdefault("elevation", None)

    # -------------------------
    # Analysis period defaults
    # -------------------------
    ap = project.setdefault("analysis_period", {})
    ap.setdefault("period_start", "01-01")
    ap.setdefault("period_end", "12-31")

    # analysis_end_year + historic_years are required, but we try to coerce if present
    if "analysis_end_year" in ap and ap["analysis_end_year"] is not None:
        try:
            ap["analysis_end_year"] = int(ap["analysis_end_year"])
        except Exception:
            pass

    if "historic_years" in ap and ap["historic_years"] is not None:
        try:
            ap["historic_years"] = int(ap["historic_years"])
        except Exception:
            pass

    # Derive analysis_years if we can
    end_year = ap.get("analysis_end_year")
    hist_years = ap.get("historic_years")
    if isinstance(end_year, int) and isinstance(hist_years, int):
        # Example: end=2024, historic_years=5 -> [2020,2021,2022,2023,2024]
        start_year = end_year - hist_years + 1
        ap["analysis_years"] = list(range(start_year, end_year + 1))

    # -------------------------
    # required_parameters normalization
    # -------------------------
    cfg["required_parameters"] = [str(p) for p in _as_list(cfg.get("required_parameters")) if str(p).strip()]

    # -------------------------
    # scenarios normalization
    # -------------------------
    # Expand scenario_mode/custom_scenarios into cfg["scenarios"] (or keep explicit list).
    cfg["scenarios"] = expand_scenarios(cfg)

    # -------------------------
    # weather_thresholds normalization
    # -------------------------
    # Canonical internal shape is scenario-first. Older configs (param-first) are converted.
    cfg["weather_thresholds"] = normalize_weather_thresholds(cfg.get("weather_thresholds"))
    # Ensure every configured scenario has a thresholds block (may be empty => no constraints).
    if isinstance(cfg.get("weather_thresholds"), dict):
        for scen in cfg.get("scenarios", []) or ["base"]:
            cfg["weather_thresholds"].setdefault(str(scen), {})

    # -------------------------
    # operational_window defaults
    # -------------------------
    ow = cfg.get("operational_window")
    if not isinstance(ow, dict):
        ow = {}
        cfg["operational_window"] = ow

    ow.setdefault("calendar_model", "all_days")
    ow.setdefault("daylight_model", "none")

    # Time bounds:
    # We support:
    #  - fixed_time -> fixed_time
    #  - sunrise    -> fixed_time
    #  - fixed_time -> sunset
    #  - sunrise    -> sunset
    #
    # Canonical structure:
    # operational_window:
    #   time_bounds:
    #     start: fixed_time|sunrise
    #     end: fixed_time|sunset
    #     start_time: "HH:MM" (if start fixed_time)
    #     end_time: "HH:MM"   (if end fixed_time)
    #
    # weekly_overrides optional:
    #   0..6: { start, end, start_time?, end_time? }
    ow.setdefault("time_bounds", {})
    tb = ow["time_bounds"]
    tb.setdefault("start", "fixed_time")
    tb.setdefault("end", "fixed_time")

    # weekly_overrides normalization: ensure keys are strings "0".."6" or ints 0..6
    if "weekly_overrides" not in ow or ow["weekly_overrides"] is None:
        ow["weekly_overrides"] = {}
    weekly = ow["weekly_overrides"]
    if not isinstance(weekly, dict):
        ow["weekly_overrides"] = {}

    # custom calendar weekdays normalization
    if ow.get("calendar_model") == "custom":
        # accept either operational_window.weekdays or operational_window.custom_weekdays
        weekdays = ow.get("weekdays", ow.get("custom_weekdays"))
        if weekdays is not None:
            ow["weekdays"] = [int(x) for x in _as_list(weekdays)]

    return cfg