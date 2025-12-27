# opticlimate/config/validate.py

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, Tuple

from opticlimate.config.schema import (
    SUPPORTED_WEATHER_PARAMETERS,
    SCENARIO_PRESETS,
    MAX_SCENARIOS,
    SCENARIO_ID_PATTERN,
    ALLOWED_UNITS,
    ALLOWED_DAYLIGHT_MODELS,
    ALLOWED_CALENDAR_MODELS,
    TIME_REFERENCE,
)

from opticlimate.config.scenarios import ALLOWED_SCENARIO_MODES


class ConfigError(ValueError):
    pass


_MMDD_RE = re.compile(r"^\d{2}-\d{2}$")
_HHMM_RE = re.compile(r"^\d{2}:\d{2}$")
_SCEN_ID_RE = re.compile(SCENARIO_ID_PATTERN)


def _require_dict(parent: Dict[str, Any], key: str, path: str) -> Dict[str, Any]:
    val = parent.get(key)
    if not isinstance(val, dict):
        raise ConfigError(f"Missing or invalid '{path}.{key}' (expected dict)")
    return val


def _require(parent: Dict[str, Any], key: str, path: str) -> Any:
    if key not in parent:
        raise ConfigError(f"Missing required '{path}.{key}'")
    return parent[key]


def _validate_mmdd(s: Any, field: str) -> None:
    if not isinstance(s, str) or not _MMDD_RE.match(s):
        raise ConfigError(f"{field} must be 'MM-DD' (e.g., '01-31'), got {s!r}")
    mm, dd = int(s[:2]), int(s[3:])
    if not (1 <= mm <= 12):
        raise ConfigError(f"{field} month out of range in {s!r}")
    if not (1 <= dd <= 31):
        raise ConfigError(f"{field} day out of range in {s!r}")


def _validate_hhmm(s: Any, field: str) -> None:
    if not isinstance(s, str) or not _HHMM_RE.match(s):
        raise ConfigError(f"{field} must be 'HH:MM' (e.g., '09:00'), got {s!r}")
    hh, mm = int(s[:2]), int(s[3:])
    if not (0 <= hh <= 23):
        raise ConfigError(f"{field} hour out of range in {s!r}")
    if not (0 <= mm <= 59):
        raise ConfigError(f"{field} minute out of range in {s!r}")


def _matches_preset(scenarios: Iterable[str], preset: str) -> bool:
    return list(scenarios) == list(SCENARIO_PRESETS[preset])


def validate_config(cfg: Dict[str, Any]) -> None:
    if not isinstance(cfg, dict):
        raise ConfigError("Config must be a dict")

    # -------------------------
    # Strict-mode legacy input rejection
    # -------------------------
    legacy = cfg.get('_legacy_inputs')
    if isinstance(legacy, dict) and legacy:
        if legacy.get('scenarios_key'):
            raise ConfigError("Legacy key 'scenarios' is not supported. Use 'scenario_mode' (+ 'custom_scenarios' for option 3).")
        if legacy.get('operational_window_flat'):
            raise ConfigError("Legacy operational_window keys (start/end/start_time/end_time at top level) are not supported. Use operational_window.time_bounds.")
        if legacy.get('weather_thresholds_param_first'):
            raise ConfigError("Legacy param-first weather_thresholds is not supported. Use scenario-first: weather_thresholds:<scenario>:<param>:{min?,max?}.")

    # -------------------------
    # project
    # -------------------------
    project = cfg.get("project")
    if not isinstance(project, dict):
        raise ConfigError("Missing or invalid 'project' block (expected dict)")

    # Optional but we enforce existence (even if empty string) for consistency
    if "activity_type" not in project:
        raise ConfigError("Missing 'project.activity_type' (free text; can be empty)")

    # units
    units = project.get("units", None)
    if units not in ALLOWED_UNITS:
        raise ConfigError(f"Invalid project.units {units!r}. Allowed: {sorted(ALLOWED_UNITS)}")

    # granularity
    gran = project.get("granularity", None)
    if gran != "hourly":
        raise ConfigError("project.granularity must be 'hourly' in v1")

    # location
    loc = _require_dict(project, "location", "project")
    for k in ("latitude", "longitude", "timezone"):
        v = loc.get(k)
        if v in (None, "", []):
            raise ConfigError(f"Missing required 'project.location.{k}'")
    try:
        float(loc["latitude"])
        float(loc["longitude"])
    except Exception as exc:
        raise ConfigError("project.location.latitude/longitude must be numbers") from exc

    # elevation optional
    if "elevation" in loc and loc["elevation"] not in (None, ""):
        try:
            float(loc["elevation"])
        except Exception as exc:
            raise ConfigError("project.location.elevation must be a number if provided") from exc

    # analysis_period
    ap = _require_dict(project, "analysis_period", "project")
    period_start = _require(ap, "period_start", "project.analysis_period")
    period_end = _require(ap, "period_end", "project.analysis_period")
    _validate_mmdd(period_start, "project.analysis_period.period_start")
    _validate_mmdd(period_end, "project.analysis_period.period_end")

    end_year = _require(ap, "analysis_end_year", "project.analysis_period")
    hist = _require(ap, "historic_years", "project.analysis_period")

    if not isinstance(end_year, int):
        raise ConfigError("project.analysis_period.analysis_end_year must be an int")
    if not isinstance(hist, int):
        raise ConfigError("project.analysis_period.historic_years must be an int")
    if hist < 1:
        raise ConfigError("project.analysis_period.historic_years must be >= 1")

    # analysis_years derived
    years = ap.get("analysis_years")
    if not isinstance(years, list) or not years:
        raise ConfigError("project.analysis_period.analysis_years must be derived during normalization")
    if years[-1] != end_year:
        raise ConfigError("project.analysis_period.analysis_years must end with analysis_end_year")

    # -------------------------
    # required_parameters
    # -------------------------
    params = cfg.get("required_parameters")
    if not isinstance(params, list) or not params:
        raise ConfigError("required_parameters must be a non-empty list")
    for p in params:
        if not isinstance(p, str) or not p.strip():
            raise ConfigError("required_parameters entries must be non-empty strings")

    invalid = set(params) - SUPPORTED_WEATHER_PARAMETERS
    if invalid:
        raise ConfigError(f"Unsupported required_parameters: {sorted(invalid)}")

    # -------------------------
    # scenarios
    # -------------------------
    # Normalization produces cfg["scenarios"]. We validate against the 3 supported options:
    #   1) ["base"]
    #   2) ["base", "conservative", "optimistic"]
    #   3) option 2 + custom ids (<= MAX_SCENARIOS total)
    scenarios = cfg.get("scenarios")
    mode = cfg.get("scenario_mode")
    if not isinstance(mode, str) or not mode.strip():
        raise ConfigError("scenario_mode is required and must be one of: base_only, standard_3, standard_3_plus")
    mode = mode.strip()
    if mode not in SCENARIO_PRESETS and mode != "standard_3_plus":
        raise ConfigError(f"Unknown scenario_mode {mode!r}. Allowed: base_only, standard_3, standard_3_plus")
    if not isinstance(scenarios, list) or not scenarios:
        raise ConfigError("scenarios must be a non-empty list")

    if len(scenarios) > MAX_SCENARIOS:
        raise ConfigError(f"Too many scenarios ({len(scenarios)}). Max allowed is {MAX_SCENARIOS}.")

    for s in scenarios:
        if not isinstance(s, str) or not s.strip():
            raise ConfigError("scenarios entries must be non-empty strings")
        if not _SCEN_ID_RE.match(s):
            raise ConfigError(
                f"Invalid scenario id {s!r}. Allowed pattern: {SCENARIO_ID_PATTERN}"
            )

    # If scenario_mode is used, validate it and any custom_scenarios usage.
    mode = cfg.get("scenario_mode")
    if mode not in (None, "", []):
        if not isinstance(mode, str):
            raise ConfigError("scenario_mode must be a string if provided")
        if mode not in ALLOWED_SCENARIO_MODES:
            raise ConfigError(f"Invalid scenario_mode {mode!r}. Allowed: {sorted(ALLOWED_SCENARIO_MODES)}")
        if mode != "standard_3_plus":
            # custom_scenarios should be empty/absent in other modes
            extras = cfg.get("custom_scenarios")
            if extras not in (None, [], ""):
                if isinstance(extras, list) and len(extras) == 0:
                    pass
                else:
                    raise ConfigError("custom_scenarios is only allowed when scenario_mode=='standard_3_plus'")

    # Enforce the allowed scenario set semantics (regardless of whether user used scenario_mode).

    expected: list[str]
    if mode == "base_only":
        expected = list(SCENARIO_PRESETS["base_only"])
    elif mode == "standard_3":
        expected = list(SCENARIO_PRESETS["standard_3"])
    else:  # standard_3_plus
        expected = list(SCENARIO_PRESETS["standard_3"])
        custom = cfg.get("custom_scenarios", [])
        if custom is None:
            custom = []
        if not isinstance(custom, list):
            raise ConfigError("custom_scenarios must be a list of scenario ids")
        # Validate ids and build expected list (deduped)
        for s in custom:
            if not isinstance(s, str) or not s.strip():
                raise ConfigError("custom_scenarios entries must be non-empty strings")
        expected += [s.strip() for s in custom]

        # De-dupe while preserving order
        dedup: list[str] = []
        for s in expected:
            if s not in dedup:
                dedup.append(s)
        expected = dedup

        if len(expected) > MAX_SCENARIOS:
            raise ConfigError(f"Too many scenarios ({len(expected)}). Max is {MAX_SCENARIOS}.")

    if scenarios != expected:
        raise ConfigError(
            "Scenario set must be declared via scenario_mode/custom_scenarios and resolve to one of the allowed options. "
            f"Expected scenarios={expected}, got {scenarios}."
        )


    # -------------------------
    # weather_thresholds
    # -------------------------
    # Canonical internal shape is scenario-first:
    #   weather_thresholds:
    #     <scenario_id>:
    #       <param>:
    #         min?: number
    #         max?: number
    #
    # For each param/scenario, min/max are optional. If neither is provided, that
    # parameter imposes no constraint.
    wt = cfg.get("weather_thresholds", {})
    if not isinstance(wt, dict):
        raise ConfigError("weather_thresholds must be a dict")

    # Every configured scenario may have a threshold block; normalization typically
    # fills missing scenarios with empty dicts (meaning: no constraints).
    for scen in scenarios:
        if scen in wt and not isinstance(wt.get(scen), dict):
            raise ConfigError(f"weather_thresholds.{scen} must be a dict")

    # Validate all provided blocks (including extra blocks not in scenarios, which we disallow).
    extra_blocks = set(wt.keys()) - set(scenarios)
    if extra_blocks:
        raise ConfigError(
            f"weather_thresholds contains scenarios not in cfg.scenarios: {sorted(extra_blocks)}"
        )

    for scen, per_scen in wt.items():
        if not isinstance(scen, str) or not scen.strip():
            raise ConfigError("weather_thresholds scenario keys must be non-empty strings")
        if not _SCEN_ID_RE.match(scen):
            raise ConfigError(f"Invalid weather_thresholds scenario key {scen!r}")
        if not isinstance(per_scen, dict):
            raise ConfigError(f"weather_thresholds.{scen} must be a dict")

        for param, bounds in per_scen.items():
            if not isinstance(param, str) or not param.strip():
                raise ConfigError(f"weather_thresholds.{scen} parameter keys must be strings")
            if param not in SUPPORTED_WEATHER_PARAMETERS:
                raise ConfigError(f"Unsupported weather parameter in thresholds: {param!r}")
            if not isinstance(bounds, dict):
                raise ConfigError(f"weather_thresholds.{scen}.{param} must be a dict")

            mn = bounds.get("min") if "min" in bounds else None
            mx = bounds.get("max") if "max" in bounds else None

            if mn is not None and not isinstance(mn, (int, float)):
                raise ConfigError(f"weather_thresholds.{scen}.{param}.min must be a number if provided")
            if mx is not None and not isinstance(mx, (int, float)):
                raise ConfigError(f"weather_thresholds.{scen}.{param}.max must be a number if provided")
            if isinstance(mn, (int, float)) and isinstance(mx, (int, float)) and float(mn) > float(mx):
                raise ConfigError(f"weather_thresholds.{scen}.{param} must satisfy min <= max")

    # -------------------------
    # operational_window
    # -------------------------
    ow = cfg.get("operational_window")
    if not isinstance(ow, dict):
        raise ConfigError("operational_window is required and must be a dict")

    cal = ow.get("calendar_model")
    if cal not in ALLOWED_CALENDAR_MODELS:
        raise ConfigError(f"Invalid operational_window.calendar_model {cal!r}")

    dl = ow.get("daylight_model")
    if dl not in ALLOWED_DAYLIGHT_MODELS:
        raise ConfigError(f"Invalid operational_window.daylight_model {dl!r}")

    # custom calendar requires explicit weekdays
    if cal == "custom":
        weekdays = ow.get("weekdays")
        if not isinstance(weekdays, list) or not weekdays:
            raise ConfigError("operational_window.weekdays must be a non-empty list when calendar_model=='custom'")
        for d in weekdays:
            if not isinstance(d, int) or d < 0 or d > 6:
                raise ConfigError("operational_window.weekdays must contain ints 0..6")

    # time bounds required
    tb = ow.get("time_bounds")
    if not isinstance(tb, dict):
        raise ConfigError("operational_window.time_bounds is required and must be a dict")

    start = tb.get("start")
    end = tb.get("end")
    if start not in TIME_REFERENCE:
        raise ConfigError(f"operational_window.time_bounds.start must be one of {sorted(TIME_REFERENCE)}")
    if end not in TIME_REFERENCE:
        raise ConfigError(f"operational_window.time_bounds.end must be one of {sorted(TIME_REFERENCE)}")

    # Enforce allowed combinations:
    # 1) fixed_time -> fixed_time
    # 2) sunrise    -> fixed_time
    # 3) fixed_time -> sunset
    # 4) sunrise    -> sunset
    if start == "sunset":
        raise ConfigError("operational_window.time_bounds.start cannot be 'sunset'")
    if end == "sunrise":
        raise ConfigError("operational_window.time_bounds.end cannot be 'sunrise'")

    # Required times when using fixed_time
    if start == "fixed_time":
        st = tb.get("start_time")
        if st is None:
            raise ConfigError("operational_window.time_bounds.start_time required when start=='fixed_time'")
        _validate_hhmm(st, "operational_window.time_bounds.start_time")

    if end == "fixed_time":
        et = tb.get("end_time")
        if et is None:
            raise ConfigError("operational_window.time_bounds.end_time required when end=='fixed_time'")
        _validate_hhmm(et, "operational_window.time_bounds.end_time")

    # Disallow degenerate sun-only windows (sunrise->sunrise, sunset->sunset)
    if start in ("sunrise", "sunset") and end == start:
        raise ConfigError("operational_window.time_bounds cannot have start == end for sun events")

    # weekly overrides are optional, but if present validate shape
    weekly = ow.get("weekly_overrides", {})
    if weekly is None:
        weekly = {}
    if not isinstance(weekly, dict):
        raise ConfigError("operational_window.weekly_overrides must be a dict if provided")

    for day_key, override in weekly.items():
        # accept day_key as int or string of int
        try:
            d = int(day_key)
        except Exception as exc:
            raise ConfigError("weekly_overrides keys must be weekday numbers 0..6") from exc
        if d < 0 or d > 6:
            raise ConfigError("weekly_overrides keys must be in range 0..6")
        if not isinstance(override, dict):
            raise ConfigError(f"weekly_overrides[{day_key}] must be a dict")

        os = override.get("start", start)
        oe = override.get("end", end)
        if os not in TIME_REFERENCE:
            raise ConfigError(f"weekly_overrides[{day_key}].start invalid")
        if oe not in TIME_REFERENCE:
            raise ConfigError(f"weekly_overrides[{day_key}].end invalid")

        if os == "sunset":
            raise ConfigError(f"weekly_overrides[{day_key}].start cannot be 'sunset'")
        if oe == "sunrise":
            raise ConfigError(f"weekly_overrides[{day_key}].end cannot be 'sunrise'")

        if os == "fixed_time":
            st = override.get("start_time")
            if st is None:
                raise ConfigError(f"weekly_overrides[{day_key}].start_time required when start=='fixed_time'")
            _validate_hhmm(st, f"weekly_overrides[{day_key}].start_time")

        if oe == "fixed_time":
            et = override.get("end_time")
            if et is None:
                raise ConfigError(f"weekly_overrides[{day_key}].end_time required when end=='fixed_time'")
            _validate_hhmm(et, f"weekly_overrides[{day_key}].end_time")