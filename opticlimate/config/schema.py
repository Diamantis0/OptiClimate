# opticlimate/config/schema.py

"""
Canonical schema constants for OptiClimate.

This module defines:
- which weather parameters users may select (Open-Meteo supported)
- allowed scenario presets
- allowed units
- allowed operational window semantics
"""

# -------------------------------------------------------------------
# Weather parameters (Open-Meteo supported, user-selectable)
# -------------------------------------------------------------------
# NOTE:
# - These are *selection options*, not free text
# - Fetcher must know how to map each parameter to Open-Meteo fields
SUPPORTED_WEATHER_PARAMETERS = {
    # Temperature
    "temperature",
    "apparent_temperature",

    # Wind
    "wind_speed",
    "wind_gusts",
    "wind_direction",

    # Precipitation
    "precipitation",
    "rain",
    "snowfall",
    "snow_depth",

    # Clouds & radiation
    "cloud_cover",
    "shortwave_radiation",
    "direct_radiation",

    # Humidity & pressure
    "relative_humidity",
    "dew_point",
    "surface_pressure",

    # Visibility & weather codes
    "visibility",
    "weather_code",
}

# -------------------------------------------------------------------
# Scenario presets / modes
# -------------------------------------------------------------------
# Users may choose one of three scenario-set options:
#   1) base_only:       ["base"]
#   2) standard_3:      ["base", "conservative", "optimistic"]
#   3) standard_3_plus: option 2 + user-provided custom scenario ids (capped)
#
# Threshold values are always explicitly provided by the user; presets only define names.
SCENARIO_PRESETS = {
    "base_only": ["base"],
    "standard_3": ["base", "conservative", "optimistic"],
}

# Backward-compatible alias used by older configs / docs
SCENARIO_PRESETS["three_scenarios"] = ["optimistic", "base", "conservative"]

# Maximum number of scenarios allowed in a single run (incl. base)
MAX_SCENARIOS = 10

# Scenario id format guardrail (validation enforces this)
SCENARIO_ID_PATTERN = r"^[A-Za-z0-9_-]+$"

# -------------------------------------------------------------------
# Units
# -------------------------------------------------------------------
ALLOWED_UNITS = {"metric", "imperial"}

# -------------------------------------------------------------------
# Daylight handling
# -------------------------------------------------------------------
# civil_twilight REMOVED (by design decision)
ALLOWED_DAYLIGHT_MODELS = {
    "none",
    "daylight",
}

# -------------------------------------------------------------------
# Calendar / operating days
# -------------------------------------------------------------------
ALLOWED_CALENDAR_MODELS = {
    "all_days",
    "mon_fri",
    "custom",     # user provides explicit weekday mapping
}

# -------------------------------------------------------------------
# Time reference semantics for operational windows
# -------------------------------------------------------------------
# Supported combinations (validated in operational_window logic):
#
# 1) fixed_time  -> fixed_time
# 2) sunrise     -> fixed_time
# 3) fixed_time  -> sunset
# 4) sunrise     -> sunset
#
TIME_REFERENCE = {
    "fixed_time",
    "sunrise",
    "sunset",
}
