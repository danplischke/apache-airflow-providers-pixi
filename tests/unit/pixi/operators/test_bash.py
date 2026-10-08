"""Unit tests for PixiBashOperator and @task.pixi_bash; a fake pixi runs the command with this machine's bash."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.sdk import DAG, task

from airflow.providers.pixi.operators.bash import PixiBashOperator


def make(fake_pixi, tmp_path: Path, **kwargs) -> PixiBashOperator:
    kwargs.setdefault("pixi_project_path", str(tmp_path))
    return PixiBashOperator(task_id="t", pixi_binary=str(fake_pixi.path), **kwargs)


def test_the_whole_command_runs_through_pixi_run_bash(fake_pixi, tmp_path: Path) -> None:
    op = make(
        fake_pixi, tmp_path, bash_command="echo $GREETING | tr a-z A-Z && pwd", env={"GREETING": "hi"}, append_env=True
    )
    assert op.execute({}) == os.path.realpath(tmp_path)
    call = fake_pixi.calls[-1]
    assert call["argv"] == [
        "run",
        "--manifest-path",
        str(tmp_path),
        "bash",
        "-c",
        "echo $GREETING | tr a-z A-Z && pwd",
    ]
    # the command is restored, so the rendered template shows what the DAG asked for
    assert op.bash_command == "echo $GREETING | tr a-z A-Z && pwd"
    assert op.cwd is None


def test_cwd_wins_over_the_manifest_directory(fake_pixi, tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    assert make(fake_pixi, tmp_path, bash_command="pwd", cwd=str(workdir)).execute({}) == os.path.realpath(workdir)


def test_environment_is_templated(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, tmp_path, bash_command="true", environment="{{ params.env }}")
    assert "environment" in op.template_fields
    op.render_template_fields({"params": {"env": "cuda"}})
    op.execute({})
    assert fake_pixi.calls[-1]["argv"][3:5] == ["--environment", "cuda"]


def test_inline_manifest_is_written_and_removed(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, tmp_path, pixi_project_path=None, requirements=["pandas"], bash_command="cat pixi.toml")
    assert op.execute({}) == '"pandas" = "*"'
    assert not Path(fake_pixi.calls[-1]["argv"][2]).exists()


def test_exit_code_99_skips_and_others_fail(fake_pixi, tmp_path: Path) -> None:
    with pytest.raises(AirflowSkipException):
        make(fake_pixi, tmp_path, bash_command="exit 99").execute({})
    with pytest.raises(AirflowException, match="non-zero exit code 3"):
        make(fake_pixi, tmp_path, bash_command="exit 3").execute({})


def test_pixi_settings_come_from_default_args(fake_pixi, tmp_path: Path) -> None:
    with DAG("d", default_args={"pixi_binary": str(fake_pixi.path), "environment": "test"}):
        op = PixiBashOperator(task_id="t", pixi_project_path=str(tmp_path), bash_command="true")
    assert op.pixi_binary == str(fake_pixi.path)
    assert op.environment == "test"


def test_task_pixi_bash_runs_the_returned_command(fake_pixi, tmp_path: Path) -> None:
    with DAG("d") as dag:

        @task.pixi_bash(pixi_project_path=str(tmp_path), pixi_binary=str(fake_pixi.path))
        def greet(name: str) -> str:
            return f"echo hello {name} from {{{{ task.task_id }}}}"

        greet("pixi")

    op = dag.get_task("greet")
    assert type(op).__name__ == "PixiBashDecoratedOperator"
    assert op.execute({"task": op}) == "hello pixi from greet"
    assert fake_pixi.calls[-1]["argv"][3:5] == ["bash", "-c"]
