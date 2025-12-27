# opticlimate/config/thresholds.py

"""Weather-threshold normalization helpers (strict).

Specification
-------------
Thresholds are *scenario-first* and bounds are optional:

weather_thresholds:
  base:
    temperature:   {min: -10, max: 35}
    wind_speed:    {max: 20}
    precipitation: {max: 2.0}
  conservative:
    wind_speed:    {max: 15}
  optimistic:
    precipitation: {}

- The top-level keys under weather_thresholds are scenario ids.
- Under each scenario, keys are weather parameter names (must be supported).
- For each (scenario, parameter), both `min` and `max` are optional.
  If neither is provided, that parameter imposes no constraint in that scenario.

This module is intentionally **strict**: it does not accept legacy param-first layouts.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Optional, Tuple


def normalize_weather_thresholds(weather_thresholds: Any) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Return thresholds in scenario-first shape (strict).

    Input shape (required):
        {scenario_id: {param: {"min"?: number, "max"?: number}}}

    Output shape:
        same as input, but:
          - guaranteed dict nesting
          - shallow-copied (safe to mutate by caller if needed)

    Notes:
      - This function does not validate parameter names or bound types;
        validation is performed in config/validate.py.
    """
    if weather_thresholds is None:
        return {}
    if not isinstance(weather_thresholds, dict):
        raise TypeError("weather_thresholds must be a dict (scenario-first)")

    wt = deepcopy(weather_thresholds)
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for scen, per_scen in wt.items():
        if scen is None:
            continue
        scen_id = str(scen).strip()
        if not scen_id:
            continue
        if not isinstance(per_scen, dict):
            # Treat non-dict as empty scenario block; validator will complain if needed.
            out[scen_id] = {}
            continue

        scen_out: Dict[str, Dict[str, Any]] = {}
        for param, bounds in per_scen.items():
            if param is None:
                continue
            param_id = str(param).strip()
            if not param_id:
                continue
            scen_out[param_id] = bounds if isinstance(bounds, dict) else {}
        out[scen_id] = scen_out

    return out


def bounds_for(
    weather_thresholds: Dict[str, Any],
    *,
    scenario: str,
    param: str,
) -> Tuple[Optional[float], Optional[float]]:
    """Get (min, max) bounds for (scenario, param) from scenario-first thresholds.

    Returns (None, None) if no constraint is specified.
    """
    if not isinstance(weather_thresholds, dict):
        return None, None

    per_scen = weather_thresholds.get(scenario, {})
    if not isinstance(per_scen, dict):
        return None, None

    bounds = per_scen.get(param, {})
    if not isinstance(bounds, dict):
        return None, None

    mn = bounds.get("min")
    mx = bounds.get("max")

    mn_f = float(mn) if isinstance(mn, (int, float)) else None
    mx_f = float(mx) if isinstance(mx, (int, float)) else None
    return mn_f, mx_f
