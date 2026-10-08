"""End-to-end test through Airflow's real task runner (``dag.test()``).

Covers what the operator-level tests cannot: a function defined in a DAG file that Airflow imports
under a generated module name, XCom passing into PixiOperator, and templated manifest paths.

Requires pixi on PATH and a migrated Airflow metadata DB::

    export AIRFLOW_HOME=/tmp/airflow-e2e AIRFLOW__CORE__LOAD_EXAMPLES=False
    export AIRFLOW__CORE__DAGS_FOLDER=$PWD/tests/system/pixi  # dag.test() needs the DAG serialized
    airflow db migrate
    PIXI_E2E_TEST=1 pytest tests/system/pixi
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("PIXI_E2E_TEST") != "1",
    reason="Set PIXI_E2E_TEST=1 (needs pixi on PATH and a migrated Airflow DB)",
)

DAG_FILE = Path(__file__).parent / "example_pixi.py"

REPORT = """
import json, sys


def write(data, out):
    with open(out, "w") as f:
        json.dump({"data": data, "prefix": sys.prefix}, f)
    return data["value"]
"""


@pytest.fixture(scope="module")
def dags():
    # generated module name, as Airflow's DAG bundle loader does
    spec = importlib.util.spec_from_file_location("unusual_prefix_e2e_example_pixi", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_dag_run(dags, tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    (project / "pixi.toml").write_text(
        '[workspace]\nname = "e2e"\nchannels = ["conda-forge"]\n'
        'platforms = ["linux-64", "osx-64", "osx-arm64"]\n\n[dependencies]\npython = "3.11.*"\n'
    )
    (project / "report.py").write_text(REPORT)
    out = tmp_path / "report.json"

    dr = dags.dag_ok.test(run_conf={"project": str(project), "out": str(out)})
    assert str(dr.state) == "success"

    report = json.loads(out.read_text())
    assert report["data"]["value"] == 40  # produce -> double (in its own environment) -> report
    assert Path(report["data"]["prefix"]).resolve() != Path(sys.prefix).resolve()
    assert Path(report["prefix"]).resolve() == (project / ".pixi" / "envs" / "default").resolve()


def test_failed_task_fails_the_dag_run(dags) -> None:
    assert str(dags.dag_fail.test().state) == "failed"
