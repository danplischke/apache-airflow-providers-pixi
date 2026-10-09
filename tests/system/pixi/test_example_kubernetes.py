"""End-to-end test of PixiKubernetesPodOperator in real pods, through Airflow's task runner (``dag.test()``).

Covers what the unit tests, which run the pod's script locally, cannot: the official pixi image, the XCom
sidecar, the triggerer of ``deferrable=True``, and the container's termination message.

Requires a Kubernetes cluster in the default kubeconfig (``kind create cluster``) that can pull
``DEFAULT_IMAGE`` and reach conda-forge, and a migrated Airflow metadata DB::

    export AIRFLOW_HOME=/tmp/airflow-k8s AIRFLOW__CORE__LOAD_EXAMPLES=False
    export AIRFLOW__CORE__DAGS_FOLDER=$PWD/tests/system/pixi
    airflow db migrate
    PIXI_K8S_E2E_TEST=1 pytest tests/system/pixi/test_example_kubernetes.py
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("PIXI_K8S_E2E_TEST") != "1",
    reason="Set PIXI_K8S_E2E_TEST=1 (needs a Kubernetes cluster in the kubeconfig and a migrated Airflow DB)",
)

DAG_FILE = Path(__file__).parent / "example_pixi_kubernetes.py"


@pytest.fixture(scope="module")
def dags():
    spec = importlib.util.spec_from_file_location("unusual_prefix_e2e_example_pixi_kubernetes", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def states(dag_run) -> dict[str, str]:
    return {ti.task_id: str(ti.state) for ti in dag_run.get_task_instances()}


def test_pod_results_reach_xcom(dags) -> None:
    dr = dags.dag_ok.test()
    assert states(dr) == dict.fromkeys(["summarize", "pickled", "deferred", "no_xcom", "check"], "success")
    assert str(dr.state) == "success"


def test_an_exception_in_the_pod_fails_the_task_with_its_message(dags, tmp_path: Path) -> None:
    dr = dags.dag_fail.test(run_conf={"out": str(tmp_path)})
    assert str(dr.state) == "failed"
    for task_id in ("boom", "boom_deferred"):
        assert (
            tmp_path / f"{task_id}.txt"
        ).read_text() == "PixiCallableError: boom raised ValueError: no rows in sales"
