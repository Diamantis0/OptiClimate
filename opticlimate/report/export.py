# opticlimate/report/export.py

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from opticlimate.report.schemas import AggregationBundle
from opticlimate.utils.run_id import sanitize_run_id


class ExportError(RuntimeError):
    pass


def _safe_rmtree(path: Path) -> None:
    """Best-effort delete for paths we own."""
    if not path.exists():
        return
    if path.is_symlink():
        # Never follow symlinks for safety.
        path.unlink(missing_ok=True)
        return
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def write_meta_json(meta: Mapping[str, Any], out_path: str | Path) -> None:
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(meta, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_tables_csv(tables: Mapping[str, pd.DataFrame], out_dir: str | Path) -> None:
    d = Path(out_dir)
    d.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        p = d / f"{name}.csv"
        df.to_csv(p, index=False)


def write_tables_parquet(tables: Mapping[str, pd.DataFrame], out_dir: str | Path) -> None:
    d = Path(out_dir)
    d.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        p = d / f"{name}.parquet"
        try:
            # engine selected by pandas (pyarrow recommended)
            df.to_parquet(p, index=False)
        except (ImportError, ModuleNotFoundError, ValueError) as exc:
            raise ExportError(
                "Parquet export requires a parquet engine (recommended: pyarrow). "
                "Install with: pip install pyarrow"
            ) from exc


def export_bundle(
    bundle: AggregationBundle,
    *,
    run_id: str | None = None,
    out_root: str | Path = "outputs",
    overwrite: bool = True,
    formats: Sequence[str] = ("parquet", "csv"),
) -> Path:
    """Materialize an AggregationBundle to disk.

    Output layout:
      outputs/<sanitized_run_id>/
        meta.json
        tables_csv/<table>.csv
        tables_parquet/<table>.parquet

    Overwrite semantics (when overwrite=True):
      - deletes tables_csv/ and tables_parquet/ under the run folder
      - rewrites meta.json
    """

    if run_id is None:
        rid = str(bundle.meta.get("run_id", ""))
    else:
        rid = str(run_id)

    rid_sanitized = sanitize_run_id(rid)
    if not rid_sanitized:
        raise ExportError(f"Invalid run_id {rid!r} (cannot sanitize to a non-empty folder name)")

    fmt = tuple(str(f).lower() for f in formats)
    allowed = {"csv", "parquet"}
    unknown = [f for f in fmt if f not in allowed]
    if unknown:
        raise ExportError(f"Unknown export format(s): {unknown}. Allowed: {sorted(allowed)}")

    out_root = Path(out_root)
    run_dir = (out_root / rid_sanitized)

    # Safety guard: never allow deleting outside out_root.
    try:
        run_dir_resolved = run_dir.resolve()
        out_root_resolved = out_root.resolve()
    except Exception:
        run_dir_resolved = run_dir
        out_root_resolved = out_root

    if out_root_resolved not in run_dir_resolved.parents and run_dir_resolved != out_root_resolved:
        raise ExportError("Refusing to write outside out_root")

    csv_dir = run_dir / "tables_csv"
    pq_dir = run_dir / "tables_parquet"

    if overwrite and run_dir.exists():
        # Only delete subfolders we own.
        _safe_rmtree(csv_dir)
        _safe_rmtree(pq_dir)

    run_dir.mkdir(parents=True, exist_ok=True)

    # Meta: ensure it carries both raw + sanitized ids if caller didn't.
    meta: dict[str, Any] = dict(bundle.meta)
    meta.setdefault("run_id_raw", rid)
    meta.setdefault("run_id_sanitized", rid_sanitized)
    meta["run_id"] = rid_sanitized

    write_meta_json(meta, run_dir / "meta.json")

    if "csv" in fmt:
        write_tables_csv(bundle.tables, csv_dir)
    if "parquet" in fmt:
        write_tables_parquet(bundle.tables, pq_dir)

    return run_dir
