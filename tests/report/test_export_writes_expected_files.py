from __future__ import annotations

import json

import pandas as pd
import pytest

from opticlimate.report.schemas import AggregationBundle
from opticlimate.report.export import export_bundle


def _mini_bundle() -> AggregationBundle:
    meta = {
        "run_id": "My Client / London Site",
        "generated_at_utc": "2025-01-01T00:00:00Z",
    }
    tables = {
        "summary_run": pd.DataFrame({"scenario_id": ["base"], "total_hours": [24]}),
        "summary_monthly": pd.DataFrame({"month": [1], "workable_rate": [0.5]}),
    }
    return AggregationBundle(meta=meta, tables=tables)


def test_exporter_creates_expected_layout_and_overwrites(tmp_path):
    # Parquet requires an engine (pyarrow recommended). Skip parquet assertions if missing.
    has_pyarrow = True
    try:
        import pyarrow  # noqa: F401
    except Exception:
        has_pyarrow = False

    bundle = _mini_bundle()

    formats = ("csv", "parquet") if has_pyarrow else ("csv",)
    out_dir = export_bundle(bundle, out_root=tmp_path, overwrite=True, formats=formats)

    # run folder name should be sanitized
    assert out_dir.name == "my-client-london-site"

    meta_path = out_dir / "meta.json"
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["run_id"] == "my-client-london-site"
    assert meta["run_id_raw"] == "My Client / London Site"
    assert meta["run_id_sanitized"] == "my-client-london-site"

    csv_dir = out_dir / "tables_csv"
    pq_dir = out_dir / "tables_parquet"
    assert (csv_dir / "summary_run.csv").exists()
    assert (csv_dir / "summary_monthly.csv").exists()
    if has_pyarrow:
        assert (pq_dir / "summary_run.parquet").exists()
        assert (pq_dir / "summary_monthly.parquet").exists()

    # Overwrite should clear owned subfolders
    (csv_dir / "junk.txt").write_text("junk", encoding="utf-8")
    out_dir2 = export_bundle(bundle, out_root=tmp_path, overwrite=True, formats=formats)
    assert out_dir2 == out_dir
    assert not (csv_dir / "junk.txt").exists()
