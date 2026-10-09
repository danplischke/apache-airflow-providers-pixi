"""``get_provider_info`` must match provider.yaml; an invalid one makes ``import airflow`` fail."""

from __future__ import annotations

import importlib
import importlib.util
import json
from pathlib import Path

import jsonschema
import pytest
import yaml

import airflow.providers.pixi
import airflow.providers.pixi.operators
from airflow.providers.pixi.get_provider_info import get_provider_info

PROVIDER_YAML = Path(__file__).parents[3] / "provider.yaml"


def test_provider_info_matches_provider_yaml() -> None:
    expected = {k: v for k, v in yaml.safe_load(PROVIDER_YAML.read_text()).items() if k != "versions"}
    expected["description"] = expected["description"].strip()
    assert get_provider_info() == expected


def test_advertised_paths_are_importable() -> None:
    info = get_provider_info()
    for section in ("operators", "sensors", "hooks"):
        for entry in info[section]:
            for module in entry["python-modules"]:
                importlib.import_module(module)
    for connection_type in info["connection-types"]:
        module, _, attr = connection_type["hook-class-name"].rpartition(".")
        hook = getattr(importlib.import_module(module), attr)
        assert hook.conn_type == connection_type["connection-type"]
        assert hook.hook_name == connection_type["hook-name"]
    for decorator in info["task-decorators"]:
        module, _, attr = decorator["class-name"].rpartition(".")
        assert callable(getattr(importlib.import_module(module), attr))


def test_provider_info_matches_airflow_schema() -> None:
    # located without importing airflow, whose provider discovery raises on an invalid provider info
    airflow_dir = Path(importlib.util.find_spec("airflow").submodule_search_locations[0])
    schema = json.loads((airflow_dir / "provider_info.schema.json").read_text())
    jsonschema.validate(get_provider_info(), schema)


def test_task_decorators_are_registered_on_task() -> None:
    from airflow.sdk import task

    for decorator in get_provider_info()["task-decorators"]:
        module, _, attr = decorator["class-name"].rpartition(".")
        assert getattr(task, decorator["name"]) is getattr(importlib.import_module(module), attr)


@pytest.mark.parametrize("package", [airflow.providers.pixi, airflow.providers.pixi.operators])
def test_lazy_exports_are_listed_and_import(package) -> None:
    assert set(package.__all__) - {"__version__"} == set(package._EXPORTS)
    for name, module in package._EXPORTS.items():
        assert getattr(package, name) is getattr(importlib.import_module(module), name)
