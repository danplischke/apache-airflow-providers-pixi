"""Unit tests for PixiTaskOperator.

The fake pixi does not read ``[tasks]``: it runs whatever follows the manifest options as a command, as pixi does
for a name that is not a task. So the tests' tasks are commands, and they check what reaches pixi.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from airflow.sdk import DAG, task

from airflow.providers.pixi.operators.task import PixiTaskOperator
from airflow.providers.pixi.utils.compat import AirflowException, AirflowSkipException

PRINT_ARGS = "import json, sys; print(json.dumps(sys.argv[1:]))"


def make(fake_pixi, tmp_path: Path, **kwargs) -> PixiTaskOperator:
    kwargs.setdefault("pixi_project_path", str(tmp_path))
    return PixiTaskOperator(task_id="t", pixi_binary=str(fake_pixi.path), **kwargs)


def test_the_task_and_its_arguments_follow_the_manifest(fake_pixi, tmp_path: Path) -> None:
    args = ["-c", PRINT_ARGS, "a b", "$HOME", "--flag", "it's"]
    op = make(fake_pixi, tmp_path, task="python", task_args=args)
    assert json.loads(op.execute({})) == ["a b", "$HOME", "--flag", "it's"]
    assert fake_pixi.calls[-1]["argv"] == ["run", "--manifest-path", str(tmp_path), "python", *args]


def test_environment_and_lock_mode_come_before_the_task(fake_pixi, tmp_path: Path) -> None:
    make(fake_pixi, tmp_path, task="true", environment="cuda", lock_mode="locked").execute({})
    assert fake_pixi.calls[-1]["argv"][3:] == ["--environment", "cuda", "--locked", "true"]


def test_task_and_task_args_are_templated(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, tmp_path, task="{{ params.task }}", task_args=["{{ params.epochs }}", "run.sh"])
    assert {"task", "task_args"} <= set(op.template_fields)
    assert "bash_command" not in op.template_fields
    op.render_template_fields({"params": {"task": "echo", "epochs": 3}})
    assert op.execute({}) == "3 run.sh"
    assert fake_pixi.calls[-1]["argv"][3:] == ["echo", "3", "run.sh"]
    assert op.task == "echo"


def test_cwd_defaults_to_the_manifest_directory(fake_pixi, tmp_path: Path) -> None:
    manifest = tmp_path / "project" / "pixi.toml"
    manifest.parent.mkdir()
    manifest.touch()
    op = make(fake_pixi, tmp_path, pixi_project_path=None, pixi_toml_path=str(manifest), task="pwd")
    assert op.execute({}) == os.path.realpath(manifest.parent)
    workdir = tmp_path / "work"
    workdir.mkdir()
    assert make(fake_pixi, tmp_path, task="pwd", cwd=str(workdir)).execute({}) == os.path.realpath(workdir)


def test_nothing_is_written_to_the_project_directory(fake_pixi, read_only_project: Path) -> None:
    op = make(fake_pixi, read_only_project, task="bash", task_args=["run.sh"])
    op._is_inline_cmd = None
    assert op.execute({}) == "from run.sh"
    assert sorted(p.name for p in read_only_project.iterdir()) == ["pixi.toml", "run.sh"]


def test_exit_code_99_skips_and_others_fail(fake_pixi, tmp_path: Path) -> None:
    with pytest.raises(AirflowSkipException):
        make(fake_pixi, tmp_path, task="python", task_args=["-c", "raise SystemExit(99)"]).execute({})
    with pytest.raises(AirflowException, match="non-zero exit code 3"):
        make(fake_pixi, tmp_path, task="python", task_args=["-c", "raise SystemExit(3)"]).execute({})
    with pytest.raises(AirflowSkipException):
        make(fake_pixi, tmp_path, task="false", skip_on_exit_code=[1]).execute({})


def test_an_inline_manifest_is_rejected(fake_pixi, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="inline manifest"):
        make(fake_pixi, tmp_path, pixi_project_path=None, pypi_dependencies=["pandas"], task="test")


@pytest.mark.parametrize(
    ("kwargs", "error", "match"),
    [
        ({"task": ""}, ValueError, "task must be"),
        ({"task": None}, ValueError, "task must be"),
        ({"task": "test", "task_args": "--verbose"}, TypeError, r"such as \['--verbose'\]"),
    ],
)
def test_invalid_arguments_are_rejected_when_the_dag_is_parsed(fake_pixi, tmp_path: Path, kwargs, error, match):
    with pytest.raises(error, match=match):
        make(fake_pixi, tmp_path, **kwargs)


def test_task_args_with_jinja_statements_are_checked_once_rendered(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, tmp_path, task="echo", task_args="{% if params.verbose %}--verbose{% endif %}")
    op.render_template_fields({"params": {"verbose": True}})
    with pytest.raises(AirflowException, match="task_args must render to a list, not str"):
        op.execute({})
    assert fake_pixi.calls == []


def test_rendered_values_are_checked_before_pixi_runs(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, tmp_path, task="{{ params.task }}", task_args="{{ params.args }}")
    op.render_template_fields({"params": {"task": "echo", "args": "not a list"}})
    with pytest.raises(AirflowException, match="task_args must render to a list, not str"):
        op.execute({})
    op = make(fake_pixi, tmp_path, task="{{ params.task }}")
    op.render_template_fields({"params": {"task": " "}})
    with pytest.raises(AirflowException, match="empty name"):
        op.execute({})
    assert fake_pixi.calls == []


def test_task_args_can_come_from_another_task(fake_pixi, tmp_path: Path) -> None:
    with DAG("d") as dag:

        @task
        def arguments() -> list[str]:
            return ["--epochs", "3"]

        PixiTaskOperator(task_id="train", pixi_project_path=str(tmp_path), task="echo", task_args=arguments())

    assert dag.get_task("train").upstream_task_ids == {"arguments"}


def test_run_env_parameters_come_from_default_args(fake_pixi, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_PIXI_CACHE", "/shared/pixi")
    monkeypatch.setenv("AIRFLOW_VAR_API_URL", "https://api.example.com")
    default_args = {
        "pixi_binary": str(fake_pixi.path),
        "lock_mode": "frozen",
        "pixi_cache_dir_variable": "pixi_cache",
        "env_vars": {"STAGE": "prod"},
        "env_from_variables": {"API_URL": "api_url"},
    }
    with DAG("d", default_args=default_args):
        op = PixiTaskOperator(
            task_id="t",
            pixi_project_path=str(tmp_path),
            task="python",
            task_args=["-c", "import os; print(os.environ['STAGE'], os.environ['API_URL'])"],
        )
    assert op.execute({}) == "prod https://api.example.com"
    call = fake_pixi.calls[-1]
    assert call["argv"][3] == "--frozen"
    assert call["env"]["PIXI_CACHE_DIR"] == "/shared/pixi"
