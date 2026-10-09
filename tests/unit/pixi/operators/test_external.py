"""Unit tests for PixiExternalPythonOperator and @task.pixi_external.

The installed environment is a directory with a ``bin/python`` that runs the test's Python, where pixi would have
installed it; no pixi is on hand.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from airflow.sdk import DAG, task

from airflow.providers.pixi.decorators.external import pixi_external_task
from airflow.providers.pixi.exceptions import PixiCallableError
from airflow.providers.pixi.operators.external import PixiExternalPythonOperator
from airflow.providers.pixi.utils.compat import AirflowException, AirflowSkipException

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="the fake environment's python is a shell script")


def install(workspace: Path, environment: str = "default") -> Path:
    """Install a fake environment into the workspace, as ``pixi install`` would; return its prefix."""
    prefix = workspace / ".pixi" / "envs" / environment
    (prefix / "bin").mkdir(parents=True)
    python = prefix / "bin" / "python"
    python.write_text(f'#!/bin/sh\nexec {sys.executable} "$@"\n')
    python.chmod(0o755)
    (workspace / "pixi.toml").touch()
    return prefix


def run_info():
    import os

    return {
        "cwd": os.getcwd(),
        "path": os.environ["PATH"],
        "conda_prefix": os.environ["CONDA_PREFIX"],
        "pythonpath": os.environ.get("PYTHONPATH"),
        "stage": os.environ.get("STAGE"),
        "pixi_cache_dir": os.environ.get("PIXI_CACHE_DIR"),
        "uv_cache_dir": os.environ.get("UV_CACHE_DIR"),
        "pip_cache_dir": os.environ.get("PIP_CACHE_DIR"),
    }


def boom(table):
    raise ValueError(f"no rows in {table}")


def make(workspace: Path, **kwargs) -> PixiExternalPythonOperator:
    if "pixi_toml_path" not in kwargs:
        kwargs.setdefault("pixi_project_path", str(workspace))
    kwargs.setdefault("python_callable", run_info)
    return PixiExternalPythonOperator(task_id="t", pixi_binary="no-pixi-here", **kwargs)


def test_runs_with_the_python_of_the_installed_environment(tmp_path: Path, monkeypatch) -> None:
    prefix = install(tmp_path)
    monkeypatch.setenv("PYTHONPATH", "/worker/site-packages")
    info = make(tmp_path, env_vars={"STAGE": "prod"}).execute({})
    assert os.path.realpath(info["cwd"]) == os.path.realpath(tmp_path)
    assert info["path"].split(os.pathsep)[0] == str(prefix / "bin")
    assert info["conda_prefix"] == str(prefix)
    assert info["pythonpath"] is None
    assert info["stage"] == "prod"


def test_environment_chooses_the_installed_environment(tmp_path: Path) -> None:
    install(tmp_path)
    prefix = install(tmp_path, "cuda")
    assert make(tmp_path, environment="cuda").execute({})["conda_prefix"] == str(prefix)


def test_the_workspace_of_a_manifest_file_is_its_directory(tmp_path: Path) -> None:
    prefix = install(tmp_path / "project")
    op = make(tmp_path, pixi_toml_path=str(tmp_path / "project" / "pixi.toml"))
    info = op.execute({})
    assert info["conda_prefix"] == str(prefix)
    assert os.path.realpath(info["cwd"]) == os.path.realpath(tmp_path / "project")


def test_relative_paths_start_at_the_dag_file(tmp_path: Path) -> None:
    prefix = install(tmp_path / "project")
    dag_file = tmp_path / "dags" / "my_dag.py"
    dag_file.parent.mkdir()
    with DAG("d") as dag:
        op = make(tmp_path, pixi_project_path="../project")
    dag.fileloc = str(dag_file)
    assert Path(op.execute({})["conda_prefix"]).resolve() == prefix.resolve()


def test_modules_next_to_the_manifest_are_importable(tmp_path: Path) -> None:
    install(tmp_path)
    (tmp_path / "helpers.py").write_text("def triple(x):\n    return 3 * x\n")
    assert make(tmp_path, python_callable="helpers:triple", op_args=[4]).execute({}) == 12


def test_a_missing_environment_fails_with_how_to_install_it(tmp_path: Path) -> None:
    install(tmp_path)
    with pytest.raises(AirflowException, match=r"No Python at .*envs/cuda/bin/python") as e:
        make(tmp_path, environment="cuda").execute({})
    assert f"pixi install --manifest-path {tmp_path} --environment cuda --frozen" in str(e.value)


def test_an_inline_manifest_is_rejected() -> None:
    with pytest.raises(ValueError, match="inline manifest"):
        PixiExternalPythonOperator(task_id="t", python_callable=run_info, dependencies=["python"])


def test_an_exception_of_the_callable_fails_the_task_with_it(tmp_path: Path) -> None:
    install(tmp_path)
    with pytest.raises(PixiCallableError, match="^boom raised ValueError: no rows in sales$"):
        make(tmp_path, python_callable=boom, op_args=["sales"]).execute({})


def test_exit_codes_skip_or_fail(tmp_path: Path) -> None:
    install(tmp_path)
    with pytest.raises(AirflowSkipException, match="exited with code 99"):
        make(tmp_path, python_callable="sys:exit", op_args=[99], skip_on_exit_code=99).execute({})
    with pytest.raises(AirflowException, match="^The environment's Python exited with code 3"):
        make(tmp_path, python_callable="sys:exit", op_args=[3]).execute({})


def test_lock_mode_from_default_args_has_no_effect(tmp_path: Path) -> None:
    install(tmp_path)
    with DAG("d", default_args={"lock_mode": "frozen"}):
        op = make(tmp_path, python_callable="builtins:abs", op_args=[-1])
    assert op.execute({}) == 1


def test_pixi_connection_and_cache_variables_have_no_effect(tmp_path: Path, monkeypatch) -> None:
    install(tmp_path)
    for name in ("PIXI_CACHE_DIR", "UV_CACHE_DIR", "PIP_CACHE_DIR"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AIRFLOW_VAR_PIXI_CACHE", "/shared/pixi")
    monkeypatch.setenv("AIRFLOW_VAR_UV_CACHE", "/shared/uv")
    monkeypatch.setenv("AIRFLOW_VAR_PIP_CACHE", "/shared/pip")

    info = make(
        tmp_path,
        pixi_conn_id="missing_connection",
        pixi_cache_dir_variable="pixi_cache",
        uv_cache_dir_variable="uv_cache",
        pip_cache_dir_variable="pip_cache",
    ).execute({})

    assert info["pixi_cache_dir"] is None
    assert info["uv_cache_dir"] is None
    assert info["pip_cache_dir"] is None


def test_task_pixi_external_ships_the_function_without_its_decorator(tmp_path: Path) -> None:
    install(tmp_path)
    with DAG("d") as dag:

        @task.pixi_external(pixi_project_path=str(tmp_path))
        def double(x: int) -> int:
            return x * 2

        double(21)

    op = dag.get_task("double")
    assert type(op).__name__ == "PixiExternalDecoratedOperator"
    assert op.custom_operator_name == "@task.pixi_external"
    assert "@task.pixi_external" not in op.get_python_source()
    assert op.execute({}) == 42


def test_task_pixi_external_keeps_xcom_args_and_dependencies(tmp_path: Path) -> None:
    with DAG("d") as dag:

        @task
        def produce():
            return 1

        task.pixi_external(pixi_project_path=str(tmp_path))(boom)(produce())

    assert dag.get_task("boom").upstream_task_ids == {"produce"}


def test_pixi_external_task_is_the_registered_decorator() -> None:
    assert task.pixi_external is pixi_external_task
