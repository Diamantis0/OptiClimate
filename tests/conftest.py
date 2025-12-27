from __future__ import annotations

import sys
from pathlib import Path
import os

import pytest


# Ensure local "opticlimate" package is importable when the project is not installed.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def pytest_collection_modifyitems(config, items):
    """Skip tests marked 'manual' unless explicitly enabled.

    Enable by setting environment variable OPTICLIMATE_RUN_MANUAL=1.
    """
    run_manual = os.environ.get("OPTICLIMATE_RUN_MANUAL", "0") == "1"
    if run_manual:
        return
    skip_manual = pytest.mark.skip(reason="manual test (set OPTICLIMATE_RUN_MANUAL=1 to run)")
    for item in items:
        if "manual" in item.keywords:
            item.add_marker(skip_manual)
