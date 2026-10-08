"""Unit tests for PixiKubernetesPodOperator and @task.pixi_kubernetes.

The pod script runs here with ``sh`` and a fake pixi, with the environment variables the pod would get;
``KubernetesPodOperator.execute`` itself is replaced, so no cluster is needed.
"""

from __future__ import annotations

import base64
import datetime
import json
import os
import pickle
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import DAG, task

from airflow.providers.pixi.operators.kubernetes import DEFAULT_IMAGE, PixiKubernetesPodOperator
from airflow.providers.pixi.utils.pixi import MIN_PIXI_VERSION

INLINE = {"dependencies": {"python": "3.12.*"}}


def add(a, b=0):
    return a + b


def when(year: int):
    import datetime

    return datetime.date(year, 1, 2)


def make(fake_pixi, **kwargs) -> PixiKubernetesPodOperator:
    if "pixi_project_path" not in kwargs and "pixi_toml_path" not in kwargs:
        kwargs = {**INLINE, **kwargs}
    kwargs.setdefault("python_callable", add)
    return PixiKubernetesPodOperator(task_id="t", pixi_binary=str(fake_pixi.path), **kwargs)


def run_pod(op: PixiKubernetesPodOperator, tmp_path: Path) -> subprocess.CompletedProcess[str]:
    """Run the pod's script as the pod would, with its XCom directory under tmp_path."""
    op.xcom_dir = str(tmp_path / "xcom")
    env = {**os.environ, **{e.name: e.value for e in op.pod_env_vars()}}
    return subprocess.run(["sh", "-c", op.pod_script()], env=env, capture_output=True, text=True, check=False)


def xcom(tmp_path: Path):
    return json.loads((tmp_path / "xcom" / "return.json").read_text())


def test_defaults() -> None:
    op = PixiKubernetesPodOperator(task_id="t", python_callable=add, **INLINE)
    assert op.image == DEFAULT_IMAGE == f"ghcr.io/prefix-dev/pixi:{MIN_PIXI_VERSION}"
    assert {"op_args", "op_kwargs", "requirements", "image", "env_vars"} <= set(op.template_fields)
    assert not {"cmds", "arguments"} & set(op.template_fields)


def test_pod_runs_the_function_in_an_inline_environment(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, op_args=[2], op_kwargs={"b": 3}, requirements=["pandas"], environment="test")
    proc = run_pod(op, tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert xcom(tmp_path) == 5
    call = fake_pixi.calls[-1]
    assert call["argv"][3:5] == ["--environment", "test"]
    assert '"pandas" = "*"' in Path(call["argv"][2]).read_text()


def test_pod_reuses_an_inline_environment_in_env_cache_path(fake_pixi, tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    op = make(fake_pixi, env_cache_path=str(cache), op_args=[1])
    assert run_pod(op, tmp_path).returncode == 0
    assert run_pod(op, tmp_path).returncode == 0
    manifests = {call["argv"][2] for call in fake_pixi.calls}
    assert len(manifests) == 1
    assert Path(manifests.pop()).parent.parent == cache


def test_pod_runs_in_a_project_of_the_image(fake_pixi, tmp_path: Path) -> None:
    project = tmp_path / "project with space"
    project.mkdir()
    (project / "helpers.py").write_text("def triple(x):\n    return 3 * x\n")
    proc = run_pod(
        make(fake_pixi, pixi_project_path=str(project), python_callable="helpers:triple", op_args=[4]), tmp_path
    )
    assert proc.returncode == 0, proc.stderr
    assert xcom(tmp_path) == 12
    assert fake_pixi.calls[-1]["argv"][2] == str(project)
    assert os.path.realpath(fake_pixi.calls[-1]["cwd"]) == os.path.realpath(project)


def test_pod_rejects_an_image_with_an_old_pixi(make_fake_pixi, tmp_path: Path) -> None:
    proc = run_pod(make(make_fake_pixi("0.80.2"), op_args=[1]), tmp_path)
    assert proc.returncode == 1
    assert f"pixi 0.80.2 in the image is older than {MIN_PIXI_VERSION}" in proc.stderr


def test_pod_fails_when_the_function_does_not_return(fake_pixi, tmp_path: Path) -> None:
    proc = run_pod(make(fake_pixi, python_callable="sys:exit", op_args=[0]), tmp_path)
    assert proc.returncode == 1
    assert "exited without returning" in proc.stderr


def test_pod_without_xcom_push_writes_no_xcom(fake_pixi, tmp_path: Path) -> None:
    assert run_pod(make(fake_pixi, op_args=[1], do_xcom_push=False), tmp_path).returncode == 0
    assert not (tmp_path / "xcom").exists()


def test_execute_hands_the_script_to_the_pod_and_unpickles_the_result(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, python_callable=when, op_args=[2026], serializer="pickle", env_vars={"KEEP": "1"})

    def pod_execute(self, context):
        # what the pod would return through its XCom sidecar
        assert self.cmds[:2] == ["sh", "-c"]
        assert {"KEEP", "PIXI_AIRFLOW_INPUT", "PIXI_AIRFLOW_RUNNER", "PIXI_AIRFLOW_MANIFEST"} <= {
            e.name for e in self.env_vars
        }
        assert run_pod(self, tmp_path).returncode == 0
        return xcom(tmp_path)

    with patch.object(KubernetesPodOperator, "execute", pod_execute):
        assert op.execute({"ti": MagicMock()}) == datetime.date(2026, 1, 2)
    # restored, so a retry builds its pod from the DAG's arguments again
    assert op.cmds == [] and op.arguments == []
    assert op.env_vars == {"KEEP": "1"}


def test_pickled_xcom_is_a_base64_string(fake_pixi, tmp_path: Path) -> None:
    assert run_pod(make(fake_pixi, python_callable=when, op_args=[2026], serializer="pickle"), tmp_path).returncode == 0
    assert pickle.loads(base64.b64decode(xcom(tmp_path))) == datetime.date(2026, 1, 2)


def test_requirements_cannot_extend_a_project_of_the_image(fake_pixi) -> None:
    op = make(fake_pixi, pixi_project_path="/app", python_callable="m:f")
    op.requirements = ["pandas"]
    with pytest.raises(Exception, match="can only extend an inline manifest"):
        op.pod_script()


def test_task_pixi_kubernetes_ships_the_function_without_its_decorator(fake_pixi, tmp_path: Path) -> None:
    with DAG("d") as dag:

        @task.pixi_kubernetes(pixi_binary=str(fake_pixi.path), namespace="jobs", **INLINE)
        def double(x: int) -> int:
            return x * 2

        double(21)

    op = dag.get_task("double")
    assert type(op).__name__ == "PixiKubernetesDecoratedOperator"
    assert op.namespace == "jobs"
    assert "@task.pixi_kubernetes" not in op.get_python_source()
    assert run_pod(op, tmp_path).returncode == 0
    assert xcom(tmp_path) == 42
