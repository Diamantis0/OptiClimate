import copy

import pytest

from opticlimate.config.normalize import normalize_config
from opticlimate.config.validate import ConfigError, validate_config

from tests.helpers import base_cfg


def test_legacy_scenarios_key_rejected():
    raw = base_cfg()
    raw["scenarios"] = ["base", "conservative", "optimistic"]

    cfg = normalize_config(copy.deepcopy(raw))
    with pytest.raises(ConfigError, match=r"Legacy key 'scenarios'"):
        validate_config(cfg)


def test_legacy_param_first_thresholds_rejected():
    raw = base_cfg()
    # Legacy param-first shape: weather_thresholds:<param>:<scenario>:{...}
    raw["weather_thresholds"] = {"temperature": {"base": {"max": 40}}}

    cfg = normalize_config(copy.deepcopy(raw))
    with pytest.raises(ConfigError, match=r"param-first"):
        validate_config(cfg)
