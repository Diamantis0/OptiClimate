# opticlimate/config/scenarios.py

"""Scenario-set expansion utilities.

This module implements the three allowed scenario definition options:

1) base_only
   - one scenario: ["base"]

2) standard_3
   - three scenarios: ["base", "conservative", "optimistic"]

3) standard_3_plus
   - option 2 + user-provided custom scenario ids (capped to MAX_SCENARIOS total)

Notes
-----
- Scenario names are *only identifiers*; threshold values are entirely user-defined.
- Normalization expands `scenario_mode` + `custom_scenarios` into `cfg["scenarios"]`.
- Validation enforces allowed ids and caps.
"""

from __future__ import annotations

from typing import Any, Dict, List

from opticlimate.config.schema import MAX_SCENARIOS, SCENARIO_PRESETS


ALLOWED_SCENARIO_MODES = {
    "base_only",
    "standard_3",
    "standard_3_plus",
}


def _as_str_list(x: Any) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(v) for v in x]
    return [str(x)]


def expand_scenarios(cfg: Dict[str, Any]) -> List[str]:
    """Return the canonical scenario id list (strict).

    Allowed options (via cfg['scenario_mode']):
      1) base_only
         -> ["base"]

      2) standard_3
         -> ["base", "conservative", "optimistic"]

      3) standard_3_plus
         -> option (2) + cfg['custom_scenarios'] (deduped, capped to MAX_SCENARIOS total)

    Notes:
      - This function intentionally does **not** accept an explicit `scenarios:` list
        from user config. Scenario sets must be declared via `scenario_mode`
        (+ `custom_scenarios` for option 3).
      - Validation of ids/patterns and illegal combinations happens in validate.py.
    """

    mode = cfg.get("scenario_mode")
    if not isinstance(mode, str) or not mode.strip():
        mode = "base_only"
    mode = mode.strip()

    if mode == "base_only":
        scenarios = ["base"]
    elif mode == "standard_3":
        scenarios = list(SCENARIO_PRESETS["standard_3"])
    elif mode == "standard_3_plus":
        scenarios = list(SCENARIO_PRESETS["standard_3"])
        scenarios += _as_str_list(cfg.get("custom_scenarios"))
    else:
        # Leave as-is; validate.py will raise a descriptive error.
        scenarios = ["base"]

    # De-dupe while preserving order
    out: List[str] = []
    for s in scenarios:
        s = str(s).strip()
        if not s:
            continue
        if s not in out:
            out.append(s)

    return out[:MAX_SCENARIOS]

