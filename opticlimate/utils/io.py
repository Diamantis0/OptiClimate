# opticlimate/utils/io.py

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


def load_config_file(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {p}")

    suffix = p.suffix.lower()
    text = p.read_text(encoding="utf-8")

    if suffix in {".json"}:
        obj = json.loads(text)
    elif suffix in {".yml", ".yaml"}:
        try:
            import yaml  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "YAML config requires PyYAML. Install with: pip install pyyaml"
            ) from exc
        obj = yaml.safe_load(text)
    else:
        raise ValueError(f"Unsupported config extension {suffix!r}. Use .json or .yml/.yaml")

    if not isinstance(obj, dict):
        raise ValueError("Config root must be a dict/object")
    return obj
