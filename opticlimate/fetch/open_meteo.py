# opticlimate/fetch/open_meteo.py

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Sequence
from urllib.error import HTTPError

try:
    import pandas as pd
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "pandas is required for opticlimate.fetch.open_meteo. Install with: pip install pandas"
    ) from exc


# Map OptiClimate canonical parameter names -> Open-Meteo hourly field names
PARAM_TO_OPEN_METEO_HOURLY: Dict[str, str] = {
    # Temperature
    "temperature": "temperature_2m",
    "apparent_temperature": "apparent_temperature",

    # Wind
    "wind_speed": "wind_speed_10m",
    "wind_gusts": "wind_gusts_10m",
    "wind_direction": "wind_direction_10m",

    # Precipitation
    "precipitation": "precipitation",
    "rain": "rain",
    "snowfall": "snowfall",
    "snow_depth": "snow_depth",

    # Clouds & radiation
    "cloud_cover": "cloud_cover",
    "shortwave_radiation": "shortwave_radiation",
    "direct_radiation": "direct_radiation",

    # Humidity & pressure
    "relative_humidity": "relative_humidity_2m",
    "dew_point": "dew_point_2m",
    "surface_pressure": "surface_pressure",

    # Visibility & weather codes
    "visibility": "visibility",
    "weather_code": "weather_code",
}


@dataclass(frozen=True)
class OpenMeteoRequest:
    latitude: float
    longitude: float
    timezone: str
    start_date: str  # YYYY-MM-DD
    end_date: str    # YYYY-MM-DD
    parameters: Sequence[str]

    def to_url(self) -> str:
        hourly_fields: List[str] = []
        for p in self.parameters:
            if p not in PARAM_TO_OPEN_METEO_HOURLY:
                raise ValueError(f"Unsupported parameter for Open-Meteo mapping: {p!r}")
            hourly_fields.append(PARAM_TO_OPEN_METEO_HOURLY[p])

        # Choose endpoint:
        # - archive endpoint for historical ranges (end_date before today)
        # - forecast endpoint otherwise
        end_dt = date.fromisoformat(self.end_date)
        today = date.today()
        base = (
            "https://archive-api.open-meteo.com/v1/archive"
            if end_dt < today
            else "https://api.open-meteo.com/v1/forecast"
        )

        query = {
            "latitude": str(self.latitude),
            "longitude": str(self.longitude),
            "timezone": self.timezone,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "hourly": ",".join(hourly_fields),
        }
        return base + "?" + urllib.parse.urlencode(query)


def _http_get_json(url: str, timeout_s: int = 30) -> dict:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "OptiClimateV2/0.1"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8")
        return json.loads(raw)
    except HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        raise RuntimeError(
            f"Open-Meteo HTTP {e.code}: {e.reason}\nURL: {url}\nBODY: {body}"
        ) from e


def fetch_hourly_weather(
    latitude: float,
    longitude: float,
    timezone: str,
    start_date: str,
    end_date: str,
    parameters: Sequence[str],
) -> "pd.DataFrame":
    """
    Fetch hourly weather from Open-Meteo and return a canonical DataFrame:
      - 'time_utc'   timezone-aware UTC datetime
      - 'time_local' timezone-aware datetime in project timezone
      - columns named as OptiClimate canonical parameter names

    DST-safe approach:
      Request time in UTC from Open-Meteo, then convert to project timezone.
      This avoids ambiguous local timestamps during DST transitions.
    """
    req = OpenMeteoRequest(
        latitude=float(latitude),
        longitude=float(longitude),
        timezone="UTC",  # force UTC from API (DST-safe)
        start_date=str(start_date),
        end_date=str(end_date),
        parameters=list(parameters),
    )
    url = req.to_url()
    data = _http_get_json(url)

    hourly = data.get("hourly")
    if not isinstance(hourly, dict):
        raise RuntimeError("Open-Meteo response missing 'hourly' block")

    times = hourly.get("time")
    if not isinstance(times, list) or not times:
        raise RuntimeError("Open-Meteo response missing 'hourly.time'")

    # Canonical timeline: UTC-aware instants
    t = pd.to_datetime(times, utc=True)
    time_utc = pd.Series(t, name="time_utc")

    # Dual clock output: UTC + Local
    df = pd.DataFrame({"time_utc": time_utc})
    df["time_local"] = df["time_utc"].dt.tz_convert(str(timezone))

    # Add requested parameter columns (OptiClimate canonical names)
    for p in req.parameters:
        field = PARAM_TO_OPEN_METEO_HOURLY[p]
        values = hourly.get(field)
        if not isinstance(values, list):
            raise RuntimeError(f"Open-Meteo response missing hourly field {field!r}")
        if len(values) != len(df):
            raise RuntimeError(
                f"Length mismatch for {field!r}: {len(values)} vs {len(df)} time rows"
            )
        df[p] = values

    return df
