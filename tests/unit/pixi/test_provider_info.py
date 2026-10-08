"""``get_provider_info`` must match provider.yaml; an invalid one makes ``import airflow`` fail."""

from __future__ import annotations

import importlib
import importlib.util
import json
from pathlib import Path

import jsonschema
import yaml

from airflow.providers.pixi.get_provider_info import get_provider_info

PROVIDER_YAML = Path(__file__).parents[3] / "provider.yaml"


def test_provider_info_matches_provider_yaml() -> None:
    expected = {k: v for k, v in yaml.safe_load(PROVIDER_YAML.read_text()).items() if k != "versions"}
    expected["description"] = expected["description"].strip()
    assert get_provider_info() == expected


def test_advertised_paths_are_importable() -> None:
    info = get_provider_info()
    for operator in info["operators"]:
        for module in operator["python-modules"]:
            importlib.import_module(module)
    for decorator in info["task-decorators"]:
        module, _, attr = decorator["class-name"].rpartition(".")
        assert callable(getattr(importlib.import_module(module), attr))


def test_provider_info_matches_airflow_schema() -> None:
    # located without importing airflow, whose provider discovery raises on an invalid provider info
    airflow_dir = Path(importlib.util.find_spec("airflow").submodule_search_locations[0])
    schema = json.loads((airflow_dir / "provider_info.schema.json").read_text())
    jsonschema.validate(get_provider_info(), schema)
