"""Every DAG file of dev/dags and tests/system/pixi imports through Airflow's DagBag, as the DAG processor does."""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest

try:
    from airflow.dag_processing.dagbag import DagBag
except ImportError:
    from airflow.models.dagbag import DagBag

ROOT = Path(__file__).resolve().parents[3]

DAG_FILES = {
    "dev/dags/pixi_showcase.py": {"pixi_showcase", "pixi_showcase_failures"},
    "dev/dags/pixi_kubernetes_showcase.py": {"pixi_kubernetes_showcase"},
    "tests/system/pixi/example_pixi.py": {"pixi_example", "pixi_example_fail"},
    "tests/system/pixi/example_pixi_bash_sensor.py": {
        "pixi_bash_sensor_example",
        "pixi_context_example",
        "pixi_callable_error_example",
    },
    "tests/system/pixi/example_pixi_kubernetes.py": {"pixi_kubernetes_example", "pixi_kubernetes_example_fail"},
}
NEEDS = {
    "dev/dags/pixi_kubernetes_showcase.py": "airflow.providers.cncf.kubernetes",
    "tests/system/pixi/example_pixi_kubernetes.py": "airflow.providers.cncf.kubernetes",
}


def _dag_bag(path: Path) -> DagBag:
    kwargs = {"dag_folder": path, "safe_mode": False}
    if "include_examples" in inspect.signature(DagBag).parameters:
        kwargs["include_examples"] = False
    return DagBag(**kwargs)


def test_every_dag_file_is_listed() -> None:
    found = {
        str(path.relative_to(ROOT))
        for folder in ("dev/dags", "tests/system/pixi")
        for path in (ROOT / folder).glob("*.py")
        if not path.name.startswith(("test_", "__"))
    }
    assert found == set(DAG_FILES)


@pytest.mark.parametrize("dag_file", sorted(DAG_FILES))
def test_dag_file_imports(dag_file: str) -> None:
    if module := NEEDS.get(dag_file):
        pytest.importorskip(module, reason=f"{dag_file} needs {module}")

    dag_bag = _dag_bag(ROOT / dag_file)

    assert dag_bag.import_errors == {}
    assert set(dag_bag.dags) == DAG_FILES[dag_file]
    for dag in dag_bag.dags.values():
        assert dag.tasks, f"{dag.dag_id} has no tasks"
