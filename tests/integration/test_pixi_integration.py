"""Integration tests against a real ``pixi`` binary.

Needs ``pixi`` on PATH and network access to conda-forge::

    PIXI_INTEGRATION_TEST=1 pytest tests/integration

These exercise what the mocked unit tests cannot: pixi solving and installing the
environment, the runner script inside it, and the JSON result coming back.
"""

from __future__ import annotations

import importlib
import os
import shutil
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pixi_airflow.operators.pixi import PixiOperator

pytestmark = pytest.mark.skipif(
    os.environ.get("PIXI_INTEGRATION_TEST") != "1",
    reason="Set PIXI_INTEGRATION_TEST=1 to run integration tests (needs pixi on PATH)",
)

# a module in the project directory: pixi runs the callable with that directory as cwd
JOB_MODULE = "pixi_airflow_it_job"
JOB_SOURCE = """
import sys


def where(x):
    return {"prefix": sys.prefix, "x": x}
"""


@pytest.fixture(scope="module")
def pixi_project(tmp_path_factory: pytest.TempPathFactory) -> Path:
    if shutil.which("pixi") is None:
        pytest.fail("pixi is not on PATH")
    project = tmp_path_factory.mktemp("pixi_project")
    (project / "pixi.toml").write_text(
        "[workspace]\n"
        'name = "pixi-airflow-it"\n'
        'channels = ["conda-forge"]\n'
        'platforms = ["linux-64", "osx-64", "osx-arm64", "win-64"]\n'
        "\n"
        "[dependencies]\n"
        'python = "3.12.*"\n'
    )
    (project / f"{JOB_MODULE}.py").write_text(JOB_SOURCE)
    return project


def test_project_path_runs_callable_in_pixi_env(pixi_project: Path) -> None:
    op = PixiOperator(
        task_id="t",
        pixi_project_path=str(pixi_project),
        python_callable=f"{JOB_MODULE}:where",
        op_kwargs={"x": 1},
        auto_install_pixi=False,
    )
    result = op.execute({"ti": MagicMock()})
    assert result["x"] == 1
    assert Path(result["prefix"]).resolve() == (pixi_project / ".pixi" / "envs" / "default").resolve()


def test_inline_manifest() -> None:
    op = PixiOperator(
        task_id="t",
        dependencies={"python": "3.11.*"},
        python_callable="platform:python_version",
        auto_install_pixi=False,
    )
    assert op.execute({"ti": MagicMock()}).startswith("3.11.")


def test_task_pixi(pixi_project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # the function must be importable on the worker to build the task, and in the env to run it
    monkeypatch.syspath_prepend(str(pixi_project))
    job = importlib.import_module(JOB_MODULE)

    from airflow.sdk import dag, task

    @dag
    def test_dag():
        task.pixi(pixi_project_path=str(pixi_project), auto_install_pixi=False)(job.where)(5)

    op = test_dag().get_task("where")
    result = op.execute({"ti": MagicMock()})
    assert result["x"] == 5
    assert Path(result["prefix"]).resolve() == (pixi_project / ".pixi" / "envs" / "default").resolve()
