# opticlimate/report/time_features.py

from __future__ import annotations

import pandas as pd


def extract_time_features(
    df: pd.DataFrame,
    *,
    time_local_col: str = "time_local",
    add_season: bool = False,
) -> pd.DataFrame:
    """Add reusable calendar dimensions derived from a local timestamp column.

    This function is deliberately small and deterministic; it should be the *only*
    place in the codebase that defines the reporting calendar dimensions.

    Conventions
    - weekday: 0=Monday .. 6=Sunday (pandas default)
    - week_of_year: ISO week number (1..53)

    Parameters
    ----------
    df:
        Input dataframe.
    time_local_col:
        Column containing local time (timezone-aware or naive). Groupings in
        reporting should be based on this column.
    add_season:
        If True, add "season" column with values DJF/MAM/JJA/SON.

    Returns
    -------
    pd.DataFrame
        Copy of df with new columns appended.
    """
    if time_local_col not in df.columns:
        raise ValueError(f"Missing {time_local_col!r} in dataframe")

    t = df[time_local_col]
    if not pd.api.types.is_datetime64_any_dtype(t):
        raise ValueError(f"{time_local_col!r} must be datetime dtype")

    out = df.copy()
    out["year"] = t.dt.year.astype("int64")
    out["month"] = t.dt.month.astype("int64")
    out["day"] = t.dt.day.astype("int64")
    out["date_local"] = t.dt.date
    out["weekday"] = t.dt.weekday.astype("int64")
    out["hour"] = t.dt.hour.astype("int64")

    # ISO week; returns a DataFrame-like accessor; cast to int64
    out["week_of_year"] = t.dt.isocalendar().week.astype("int64")

    if add_season:
        # Meteorological seasons based on month
        m = out["month"]
        out["season"] = pd.Series(
            pd.Categorical(
                pd.cut(
                    m,
                    bins=[0, 2, 5, 8, 11, 12],
                    labels=["DJF", "MAM", "JJA", "SON", "DJF"],
                    include_lowest=True,
                    right=True,
                ),
                categories=["DJF", "MAM", "JJA", "SON"],
                ordered=True,
            ),
            index=out.index,
        ).astype(str)

    return out
