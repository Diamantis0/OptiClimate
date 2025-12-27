# opticlimate/report/build.py

from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence

import hashlib
import json

import pandas as pd

from opticlimate.report import schemas
from opticlimate.utils.run_id import sanitize_run_id
from opticlimate.report.aggregate_core import summary_monthly, summary_run, summary_yearly
from opticlimate.report.aggregate_curves import curves_cumulative_daily, curve_reliability_monthly
from opticlimate.report.aggregate_loss import loss_by_param_monthly, loss_by_param_yearly
from opticlimate.report.aggregate_stats import reliability_targets_monthly, stats_monthly_workable_rate_dist
from opticlimate.report.aggregate_visual import (
    pivot_month_x_hour_param_mean_operational,
    pivot_month_x_hour_workable_rate,
    pivot_month_x_weekday_workable_rate,
    pivot_weekday_x_hour_workable_rate,
)
from opticlimate.report.aggregate_weather import weather_stats_monthly, weather_stats_yearly
from opticlimate.report.schemas import (
    TABLE_CURVES_CUMULATIVE_DAILY,
    TABLE_CURVE_RELIABILITY_MONTHLY,
    TABLE_PIVOT_MONTH_X_HOUR_PARAM_MEAN_OPERATIONAL,
    TABLE_PIVOT_MONTH_X_WEEKDAY_WORKABLE_RATE,
    TABLE_PIVOT_WEEKDAY_X_HOUR_WORKABLE_RATE,
    TABLE_RELIABILITY_TARGETS_MONTHLY,
    TABLE_STATS_MONTHLY_WORKABLE_RATE_DIST,
)
from opticlimate.report.aggregate_streaks import aggregate_streaks_operational


def _hash_config(cfg: Mapping[str, Any]) -> str:
    """Compute a stable hash of a config mapping (best-effort)."""
    try:
        payload = json.dumps(cfg, sort_keys=True, default=str).encode("utf-8")
    except Exception:
        payload = repr(cfg).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def build_core_bundle(
    hourly: pd.DataFrame,
    *,
    cfg: Mapping[str, Any],
    run_id: str = "run",
    time_local_col: str = "time_local",
    operational_col: str = "is_operational",
    workable_col: str = "is_workable",
    limiting_col: str = "limiting_param",
    scenario_col: str = "scenario_id",
    include_weather_stats: bool = False,
    include_curves: bool = True,
    include_visual: bool = True,
    include_stats: bool = True,
    weather_params: Sequence[str] | None = None,
    visual_params_for_mean: Sequence[str] | None = None,
    validate: bool = True,
) -> schemas.AggregationBundle:
    """Build a validated AggregationBundle.

    Includes tables:
      - summary_run
      - summary_monthly
      - summary_yearly
      - loss_by_param_monthly (if limiting_col exists)
      - loss_by_param_yearly (if limiting_col exists)
      - weather_stats_monthly/yearly (Phase 5C, if include_weather_stats)
      - curves_cumulative_daily (Phase 5D, if include_curves)
      - curve_reliability_monthly (Phase 5D, if include_curves)
      - pivot_month_x_hour_workable_rate (Phase 5E, if include_visual)
      - pivot_month_x_weekday_workable_rate (Phase 5E, if include_visual)
      - pivot_weekday_x_hour_workable_rate (Phase 5E, if include_visual)
      - pivot_month_x_hour_param_mean_operational (Phase 5E, if include_visual and params provided)
      - stats_monthly_workable_rate_dist (Phase 5F, if include_stats)
      - reliability_targets_monthly (Phase 5F, if include_stats)
    """
    # hourly truth table basic validation
    # If weather stats are requested, require that weather parameter columns exist.
    _params = list(weather_params) if weather_params is not None else list(cfg.get("required_parameters", []))
    schemas.validate_hourly_truth_table(hourly, weather_params=_params if include_weather_stats else None)

    tables: Dict[str, pd.DataFrame] = {}

    # Core summaries
    tables[schemas.TABLE_SUMMARY_RUN] = summary_run(
        hourly,
        operational_col=operational_col,
        workable_col=workable_col,
        scenario_col=scenario_col,
    )
    tables[schemas.TABLE_SUMMARY_MONTHLY] = summary_monthly(
        hourly,
        time_local_col=time_local_col,
        operational_col=operational_col,
        workable_col=workable_col,
        scenario_col=scenario_col,
    )
    tables[schemas.TABLE_SUMMARY_YEARLY] = summary_yearly(
        hourly,
        time_local_col=time_local_col,
        operational_col=operational_col,
        workable_col=workable_col,
        scenario_col=scenario_col,
    )

    # Loss decomposition (weather-lost only)
    if limiting_col in hourly.columns:
        tables[schemas.TABLE_LOSS_BY_PARAM_MONTHLY] = loss_by_param_monthly(
            hourly,
            time_local_col=time_local_col,
            limiting_col=limiting_col,
            operational_col=operational_col,
            workable_col=workable_col,
            scenario_col=scenario_col,
        )
        tables[schemas.TABLE_LOSS_BY_PARAM_YEARLY] = loss_by_param_yearly(
            hourly,
            time_local_col=time_local_col,
            limiting_col=limiting_col,
            operational_col=operational_col,
            workable_col=workable_col,
            scenario_col=scenario_col,
        )

    # Weather statistics (Phase 5C)
    if include_weather_stats:
        if not _params:
            raise ValueError("include_weather_stats=True but no weather parameters were provided")
        tables[schemas.TABLE_WEATHER_STATS_MONTHLY] = weather_stats_monthly(
            hourly,
            params=_params,
            time_local_col=time_local_col,
            operational_col=operational_col,
            workable_col=workable_col,
        )
        tables[schemas.TABLE_WEATHER_STATS_YEARLY] = weather_stats_yearly(
            hourly,
            params=_params,
            time_local_col=time_local_col,
            operational_col=operational_col,
            workable_col=workable_col,
        )

    # Curves (Phase 5D)
    if include_curves:
        tables[TABLE_CURVES_CUMULATIVE_DAILY] = curves_cumulative_daily(
            hourly, time_col=time_local_col, scenario_col=scenario_col
        )
        tables[TABLE_CURVE_RELIABILITY_MONTHLY] = curve_reliability_monthly(
            hourly, time_col=time_local_col, scenario_col=scenario_col
        )

    # Visual-ready pivot datasets (Phase 5E)
    if include_visual:
        tables[schemas.TABLE_PIVOT_MONTH_X_HOUR_WORKABLE_RATE] = pivot_month_x_hour_workable_rate(
            hourly,
            time_local_col=time_local_col,
            operational_col=operational_col,
            workable_col=workable_col,
            scenario_col=scenario_col,
        )
        tables[TABLE_PIVOT_MONTH_X_WEEKDAY_WORKABLE_RATE] = pivot_month_x_weekday_workable_rate(
            hourly,
            time_local_col=time_local_col,
            operational_col=operational_col,
            workable_col=workable_col,
            scenario_col=scenario_col,
        )
        tables[TABLE_PIVOT_WEEKDAY_X_HOUR_WORKABLE_RATE] = pivot_weekday_x_hour_workable_rate(
            hourly,
            time_local_col=time_local_col,
            operational_col=operational_col,
            workable_col=workable_col,
            scenario_col=scenario_col,
        )

        mean_params = list(visual_params_for_mean) if visual_params_for_mean is not None else []
        if mean_params:
            tables[TABLE_PIVOT_MONTH_X_HOUR_PARAM_MEAN_OPERATIONAL] = pivot_month_x_hour_param_mean_operational(
                hourly,
                params=mean_params,
                time_local_col=time_local_col,
                operational_col=operational_col,
                scenario_col=scenario_col,
            )

    # Statistics (Phase 5F)
    if include_stats:
        tables[TABLE_STATS_MONTHLY_WORKABLE_RATE_DIST] = stats_monthly_workable_rate_dist(
            hourly,
            time_col=time_local_col,
            scenario_col=scenario_col,
        )
        tables[TABLE_RELIABILITY_TARGETS_MONTHLY] = reliability_targets_monthly(
            hourly,
            time_col=time_local_col,
            scenario_col=scenario_col,
        )

    # Streaks (Phase 5G)
    streak_tables = aggregate_streaks_operational(
        hourly,
        time_utc_col="time_utc",
        operational_col="operational_flag",
        workable_col="workable_flag",
        scenario_col=scenario_col,
    )
    tables.update(streak_tables)

    # Meta
    project = cfg.get("project", {}) if isinstance(cfg, Mapping) else {}
    loc = project.get("location", {}) if isinstance(project, Mapping) else {}
    tz = str(loc.get("timezone", "")) if isinstance(loc, Mapping) else ""
    loc_name = str(loc.get("name", "")) if isinstance(loc, Mapping) else ""
    analysis_period = project.get("analysis_period", {}) if isinstance(project, Mapping) else {}
    start = str(analysis_period.get("start_date", analysis_period.get("period_start", "")))
    end = str(analysis_period.get("end_date", analysis_period.get("period_end", "")))

    config_hash = _hash_config(cfg)
    run_id_raw = str(run_id)
    run_id_sanitized = sanitize_run_id(run_id_raw) or run_id_raw
    meta = schemas.make_meta_skeleton(
        run_id=run_id_sanitized,
        run_id_raw=run_id_raw,
        run_id_sanitized=run_id_sanitized,
        generated_at_utc=pd.Timestamp.utcnow().isoformat(),
        location_name=loc_name,
        timezone=tz,
        period_start_utc=start,
        period_end_utc=end,
        parameters=cfg.get("required_parameters", []),
        thresholds=cfg.get("weather_thresholds", {}),
        config_hash=config_hash,
        rows_hourly=len(hourly),
    )

    bundle = schemas.AggregationBundle(meta=meta, tables=tables)

    if validate:
        # Require what we produced.
        required: Sequence[str] = (
            schemas.TABLE_SUMMARY_RUN,
            schemas.TABLE_SUMMARY_MONTHLY,
            schemas.TABLE_SUMMARY_YEARLY,
        )
        if include_weather_stats:
            required = (*required, schemas.TABLE_WEATHER_STATS_MONTHLY, schemas.TABLE_WEATHER_STATS_YEARLY)
        if include_curves:
            required = (*required, TABLE_CURVES_CUMULATIVE_DAILY, TABLE_CURVE_RELIABILITY_MONTHLY)
        if include_visual:
            # Only require the pivot you already have schema validation for
            required = (*required, schemas.TABLE_PIVOT_MONTH_X_HOUR_WORKABLE_RATE)

        if include_stats:
            required = (*required, TABLE_STATS_MONTHLY_WORKABLE_RATE_DIST, TABLE_RELIABILITY_TARGETS_MONTHLY)

        schemas.validate_bundle(bundle, require=required)

    return bundle
