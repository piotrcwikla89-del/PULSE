import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def test_component_category_normalization():
    router = importlib.import_module("routers.komponenty")

    assert router._normalize_component_category("farba") == "FARBY"
    assert router._normalize_component_category("lakier") == "LAKIERY"
    assert router._normalize_component_category("adhesive") == "DODATKI"
    assert router._normalize_component_category("cleaner") == "CHEMIA"


def test_edit_access_for_manager_and_admin_only():
    router = importlib.import_module("routers.komponenty")

    assert router._can_edit_components({"role": "manager"}) is True
    assert router._can_edit_components({"role": "admin"}) is True
    assert router._can_edit_components({"role": "operator_mieszalni"}) is False
