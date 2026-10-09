"""Unit tests for PixiBashOperator and @task.pixi_bash; a fake pixi runs the command with this machine's bash."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from airflow.sdk import DAG, task

from airflow.providers.pixi.operators.bash import PixiBashOperator
from airflow.providers.pixi.utils.compat import AirflowException, AirflowSkipException


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
    op = make(
        fake_pixi, tmp_path, pixi_project_path=None, pypi_dependencies=["pandas"], bash_command="grep pandas pixi.toml"
    )
    assert op.execute({}) == 'pandas = "*"'
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


@pytest.mark.parametrize("bash_command", ["echo hi", "./run.sh", "bash run.sh"])
def test_nothing_is_written_to_the_project_directory(fake_pixi, read_only_project: Path, bash_command: str) -> None:
    op = make(fake_pixi, read_only_project, bash_command=bash_command)
    assert op.execute({}) in ("hi", "from run.sh")
    assert sorted(p.name for p in read_only_project.iterdir()) == ["pixi.toml", "run.sh"]


def test_task_pixi_bash_writes_nothing_to_the_project_directory(fake_pixi, read_only_project: Path) -> None:
    with DAG("d") as dag:

        @task.pixi_bash(pixi_project_path=str(read_only_project), pixi_binary=str(fake_pixi.path))
        def greet() -> str:
            return "echo hi"

        greet()

    op = dag.get_task("greet")
    op._is_inline_cmd = None
    assert op.execute({"task": op}) == "hi"
    assert op._is_inline_cmd is None
    assert sorted(p.name for p in read_only_project.iterdir()) == ["pixi.toml", "run.sh"]


def test_lock_mode_is_passed_before_bash(fake_pixi, tmp_path: Path) -> None:
    make(fake_pixi, tmp_path, bash_command="true", lock_mode="frozen").execute({})
    assert fake_pixi.calls[-1]["argv"][3:6] == ["--frozen", "bash", "-c"]


def test_run_env_parameters_are_templated_like_pixi_operator(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, tmp_path, bash_command="true")
    assert {"pixi_cache_dir_variable", "uv_cache_dir_variable", "pip_cache_dir_variable"} <= set(op.template_fields)
    assert "env_vars" not in op.template_fields


def test_run_env_parameters_come_from_default_args(fake_pixi, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_PIXI_CACHE", "/shared/pixi")
    monkeypatch.setenv("AIRFLOW_VAR_UV_CACHE", "/shared/uv")
    monkeypatch.setenv("AIRFLOW_VAR_API_URL", "https://api.example.com")
    monkeypatch.setenv("AIRFLOW_CONN_WAREHOUSE", json.dumps({"conn_type": "postgres", "host": "db.example.com"}))
    default_args = {
        "pixi_binary": str(fake_pixi.path),
        "pixi_cache_dir_variable": "pixi_cache",
        "uv_cache_dir_variable": "uv_cache",
        "env_vars": {"STAGE": "prod"},
        "env_from_variables": {"API_URL": "api_url"},
        "env_from_connections": {"DB_HOST": "warehouse.host"},
    }
    with DAG("d", default_args=default_args):
        op = PixiBashOperator(
            task_id="t", pixi_project_path=str(tmp_path), bash_command='echo "$STAGE $API_URL $DB_HOST"'
        )
    assert op.execute({}) == "prod https://api.example.com db.example.com"
    env = fake_pixi.calls[-1]["env"]
    assert env["PIXI_CACHE_DIR"] == "/shared/pixi"
    assert env["UV_CACHE_DIR"] == "/shared/uv"


def test_env_precedence(fake_pixi, tmp_path: Path, monkeypatch) -> None:
    for name in ("A", "B", "C", "D", "PIXI_CACHE_DIR"):
        monkeypatch.setenv(name, "worker")
    monkeypatch.setenv("AIRFLOW_VAR_PIXI_CACHE", "variable")
    monkeypatch.setenv("AIRFLOW_VAR_FROM_VARIABLE", "env_from_variables")
    op = make(
        fake_pixi,
        tmp_path,
        bash_command='echo "$A $B $C $D $PIXI_CACHE_DIR"',
        append_env=True,
        pixi_cache_dir_variable="pixi_cache",
        env_from_variables={"B": "from_variable", "C": "from_variable", "D": "from_variable"},
        env={"C": "env", "D": "env"},
        env_vars={"D": "env_vars"},
    )
    assert op.execute({}) == "worker env_from_variables env env_vars variable"


def test_env_without_append_env_still_gets_the_resolved_variables(fake_pixi, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("WORKER_ONLY", "worker")
    monkeypatch.setenv("AIRFLOW_VAR_PIXI_CACHE", "/shared/pixi")
    op = make(
        fake_pixi,
        tmp_path,
        bash_command='echo "[$WORKER_ONLY] $PIXI_CACHE_DIR $A"',
        env={"A": "env"},
        pixi_cache_dir_variable="pixi_cache",
    )
    assert op.execute({}) == "[] /shared/pixi env"


def test_the_command_sees_the_credentials_file_which_is_removed_afterwards(
    fake_pixi, tmp_path: Path, monkeypatch
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv(
        "AIRFLOW_CONN_CHANNEL", json.dumps({"conn_type": "pixi", "host": "repo.prefix.dev", "password": "token"})
    )
    op = make(
        fake_pixi,
        tmp_path,
        bash_command='echo "$RATTLER_AUTH_FILE $(tr -d " \\n" < "$RATTLER_AUTH_FILE")"',
        pixi_conn_id="channel",
    )
    path, content = op.execute({}).split(" ")
    assert json.loads(content) == {"repo.prefix.dev": {"BearerToken": "token"}}
    assert not os.path.exists(path)


def test_get_env_outside_execute_fails(fake_pixi, tmp_path: Path) -> None:
    with pytest.raises(AirflowException, match="only be called from execute"):
        make(fake_pixi, tmp_path, bash_command="true").get_env({})


def test_task_pixi_bash_gets_the_run_env(fake_pixi, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_API_URL", "https://api.example.com")
    with DAG("d", default_args={"env_vars": {"STAGE": "prod"}, "env_from_variables": {"API_URL": "api_url"}}) as dag:

        @task.pixi_bash(pixi_project_path=str(tmp_path), pixi_binary=str(fake_pixi.path))
        def show() -> str:
            return 'echo "$STAGE $API_URL"'

        show()

    op = dag.get_task("show")
    assert op.execute({"task": op}) == "prod https://api.example.com"


def test_the_workers_python_paths_do_not_reach_the_command(fake_pixi, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PYTHONPATH", "/worker/site-packages")
    monkeypatch.setenv("VIRTUAL_ENV", "/worker/venv")
    command = 'echo "${PYTHONPATH:-unset} ${VIRTUAL_ENV:-unset} $PYTHONNOUSERSITE"'
    assert make(fake_pixi, tmp_path, bash_command=command).execute({}) == "unset unset 1"


def test_env_can_set_pythonpath_for_the_command(fake_pixi, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PYTHONPATH", "/worker/site-packages")
    op = make(fake_pixi, tmp_path, bash_command='echo "$PYTHONPATH"', env={"PYTHONPATH": "/explicit"}, append_env=True)
    assert op.execute({}) == "/explicit"
