"""Integration tests against a real ``pixi`` binary.

Needs ``pixi`` on PATH and network access to conda-forge::

    PIXI_INTEGRATION_TEST=1 pytest tests/integration

These exercise what the unit tests' fake pixi cannot: pixi solving and installing environments, the
runner inside them, and stopping pixi together with the process it started.
"""

from __future__ import annotations

import datetime
import os
import shutil
import time
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowTaskTimeout
from airflow.sdk.execution_time.timeout import timeout

from airflow.providers.pixi.operators.pixi import PixiOperator

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

WORKSPACE = """
[workspace]
name = "pixi-airflow-it"
channels = ["conda-forge"]
platforms = ["linux-64", "osx-64", "osx-arm64", "win-64"]
"""


def run(op: PixiOperator):
    return op.execute({"ti": MagicMock()})


def where(x):
    import sys

    return {"prefix": sys.prefix, "x": x}


def heartbeat(path):
    import time

    end = time.monotonic() + 60
    while time.monotonic() < end:
        with open(path, "a") as f:
            f.write(".")
        time.sleep(0.05)


@pytest.fixture(scope="module")
def pixi_project(tmp_path_factory: pytest.TempPathFactory) -> Path:
    if shutil.which("pixi") is None:
        pytest.fail("pixi is not on PATH")
    project = tmp_path_factory.mktemp("pixi_project")
    (project / "pixi.toml").write_text(WORKSPACE + '\n[dependencies]\npython = "3.12.*"\n')
    (project / f"{JOB_MODULE}.py").write_text(JOB_SOURCE)
    return project


def env_prefix(directory: Path) -> Path:
    return (directory / ".pixi" / "envs" / "default").resolve()


def test_module_path_from_the_project(pixi_project: Path) -> None:
    op = PixiOperator(
        task_id="t",
        pixi_project_path=str(pixi_project),
        python_callable=f"{JOB_MODULE}:where",
        op_kwargs={"x": 1},
        auto_install_pixi=False,
    )
    result = run(op)
    assert result["x"] == 1
    assert Path(result["prefix"]).resolve() == env_prefix(pixi_project)


def test_function_source_runs_in_the_environment(pixi_project: Path) -> None:
    op = PixiOperator(
        task_id="t", pixi_project_path=str(pixi_project), python_callable=where, op_args=[2], auto_install_pixi=False
    )
    result = run(op)
    assert result["x"] == 2
    assert Path(result["prefix"]).resolve() == env_prefix(pixi_project)


def test_inline_manifest() -> None:
    op = PixiOperator(
        task_id="t",
        dependencies={"python": "3.11.*"},
        python_callable="platform:python_version",
        auto_install_pixi=False,
    )
    assert run(op).startswith("3.11.")


def test_toml_path_uses_that_file(tmp_path: Path) -> None:
    (tmp_path / "pixi.toml").write_text(WORKSPACE + '\n[dependencies]\npython = "3.11.*"\n')
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "it"\nversion = "0.1"\n'
        + WORKSPACE.replace("[workspace]", "[tool.pixi.workspace]")
        + '\n[tool.pixi.dependencies]\npython = "3.13.*"\n'
    )
    op = PixiOperator(
        task_id="t",
        pixi_toml_path=str(tmp_path / "pyproject.toml"),
        python_callable="platform:python_version",
        auto_install_pixi=False,
    )
    assert run(op).startswith("3.13.")


def test_env_cache_path_reuses_the_environment(tmp_path: Path) -> None:
    def op():
        return PixiOperator(
            task_id="t",
            dependencies={"python": "3.12.*"},
            python_callable=where,
            op_args=[3],
            env_cache_path=str(tmp_path),
            auto_install_pixi=False,
        )

    first = run(op())
    (env_dir,) = tmp_path.iterdir()
    assert Path(first["prefix"]).resolve() == env_prefix(env_dir)
    assert (env_dir / "pixi.lock").exists()
    assert run(op())["prefix"] == first["prefix"]


def test_pickle_serializer(pixi_project: Path) -> None:
    def next_day(day):
        import datetime

        return day + datetime.timedelta(days=1)

    op = PixiOperator(
        task_id="t",
        pixi_project_path=str(pixi_project),
        python_callable=next_day,
        op_args=[datetime.date(2026, 1, 1)],
        serializer="pickle",
        auto_install_pixi=False,
    )
    assert run(op) == datetime.date(2026, 1, 2)


def test_output_is_streamed_to_the_task_log(pixi_project: Path) -> None:
    def chatty():
        print("hello from pixi")
        return 1

    op = PixiOperator(task_id="t", pixi_project_path=str(pixi_project), python_callable=chatty, auto_install_pixi=False)
    with patch.object(PixiOperator, "log", new_callable=PropertyMock) as log:
        assert run(op) == 1
    assert "hello from pixi" in [c.args[1] for c in log.return_value.info.call_args_list if c.args[0] == "%s"]


def test_execution_timeout_stops_pixi_and_the_callable(pixi_project: Path, tmp_path: Path) -> None:
    beat = tmp_path / "beat"
    op = PixiOperator(
        task_id="t",
        pixi_project_path=str(pixi_project),
        python_callable=heartbeat,
        op_args=[str(beat)],
        auto_install_pixi=False,
    )
    started = time.monotonic()
    with pytest.raises(AirflowTaskTimeout), timeout(5):
        run(op)
    assert time.monotonic() - started < 30, "the run was not stopped"
    size = beat.stat().st_size
    time.sleep(0.5)
    assert beat.stat().st_size == size, "the callable is still running"


def test_task_pixi_with_a_function_defined_in_the_dag(pixi_project: Path) -> None:
    @dag
    def test_dag():
        @task.pixi(pixi_project_path=str(pixi_project), auto_install_pixi=False)
        def double(x: int) -> dict:
            import sys

            return {"value": x * 2, "prefix": sys.prefix}

        double(5)

    result = test_dag().get_task("double").execute({"ti": MagicMock()})
    assert result["value"] == 10
    assert Path(result["prefix"]).resolve() == env_prefix(pixi_project)
