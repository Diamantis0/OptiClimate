import copy

from opticlimate.config.normalize import normalize_config
from opticlimate.config.validate import validate_config

from tests.helpers import base_cfg


def test_standard_3_expands_correctly():
    cfg = base_cfg()
    cfg["scenario_mode"] = "standard_3"
    cfg["custom_scenarios"] = []
    cfg.pop("scenarios", None)

    cfg2 = normalize_config(copy.deepcopy(cfg))
    validate_config(cfg2)

    assert set(map(str, cfg2["scenarios"])) == {"base", "conservative", "optimistic"}
