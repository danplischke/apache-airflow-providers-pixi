"""Unit tests for PixiDockerOperator and @task.pixi_docker.

``DockerOperator.execute`` is replaced by one that runs the container's entrypoint here with ``sh`` and a fake pixi,
with the container's environment variables, then reads the result file as ``retrieve_output`` does, so no Docker
daemon is needed.
"""

from __future__ import annotations

import datetime
import os
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from airflow.providers.docker.exceptions import DockerContainerFailedException, DockerContainerFailedSkipException
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, task

from airflow.providers.pixi.decorators.docker import pixi_docker_task
from airflow.providers.pixi.exceptions import PixiCallableError
from airflow.providers.pixi.operators.container import DEFAULT_IMAGE, MAX_ENV_VALUE_BYTES
from airflow.providers.pixi.operators.docker import PixiDockerOperator
from airflow.providers.pixi.utils.compat import AirflowException, AirflowSkipException
from airflow.providers.pixi.utils.pixi import MIN_PIXI_VERSION

INLINE = {"dependencies": {"python": "3.12.*"}}


def add(a, b=0):
    return a + b


def when(year: int):
    import datetime

    return datetime.date(year, 1, 2)


def boom(table):
    raise ValueError(f"no rows in {table}")


def make(fake_pixi, tmp_path: Path, **kwargs) -> PixiDockerOperator:
    if "pixi_project_path" not in kwargs and "pixi_toml_path" not in kwargs:
        kwargs = {**INLINE, **kwargs}
    kwargs.setdefault("python_callable", add)
    op = PixiDockerOperator(task_id="t", pixi_binary=str(fake_pixi.path), **kwargs)
    op.result_path = op.retrieve_output_path = str(tmp_path / "result")
    return op


class Container:
    """Stands in for ``DockerOperator.execute``: runs the entrypoint here and records what it ran with."""

    def __init__(self, workdir: Path | None = None) -> None:
        self.workdir = workdir
        self.proc: subprocess.CompletedProcess[str] | None = None
        self.environment: dict[str, str] = {}

    def __call__(self, op: PixiDockerOperator, context) -> object:
        self.environment = {**op.environment, **op._private_environment}
        env = {**os.environ, **self.environment}
        env.pop("PWD", None)
        self.proc = subprocess.run(
            op.entrypoint, env=env, cwd=self.workdir, capture_output=True, text=True, check=False
        )
        logs = (self.proc.stdout + self.proc.stderr).splitlines()
        if self.proc.returncode in op.skip_on_exit_code:
            raise DockerContainerFailedSkipException("skipped", logs=logs)
        if self.proc.returncode != 0:
            raise DockerContainerFailedException(f"Docker container failed: {self.proc.returncode}", logs=logs)
        with open(op.retrieve_output_path, "rb") as f:
            return op.pickling_library.load(f)


def run(op: PixiDockerOperator, context=None, workdir: Path | None = None) -> tuple[object, Container]:
    container = Container(workdir)
    with patch.object(DockerOperator, "execute", lambda self, context: container(self, context)):
        return op.execute(context or {}), container


def test_defaults() -> None:
    op = PixiDockerOperator(task_id="t", python_callable=add, **INLINE)
    assert op.image == DEFAULT_IMAGE == f"ghcr.io/prefix-dev/pixi:{MIN_PIXI_VERSION}"
    assert op.retrieve_output is True
    assert op.retrieve_output_path == PixiDockerOperator.result_path
    assert op.custom_operator_name == "PixiDocker"
    assert {"op_args", "op_kwargs", "pypi_dependencies", "image", "env_vars", "environment", "mounts"} <= set(
        op.template_fields
    )
    assert "command" not in op.template_fields
    assert len(op.template_fields) == len(set(op.template_fields))


def test_container_runs_the_function_in_an_inline_environment(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, tmp_path, op_args=[2], op_kwargs={"b": 3}, pypi_dependencies=["pandas"])
    result, container = run(op)
    assert result == 5, container.proc.stderr
    call = fake_pixi.calls[-1]
    assert call["argv"][3] == "python"
    assert 'pandas = "*"' in Path(call["argv"][2]).read_text()


def test_execute_restores_the_operator(fake_pixi, tmp_path: Path) -> None:
    op = make(
        fake_pixi,
        tmp_path,
        env_vars={"KEEP": "1"},
        private_environment={"TOKEN": "secret"},
        pixi_project_path=str(tmp_path),
        environment="cuda",
        python_callable="builtins:abs",
        op_args=[-2],
    )
    result, container = run(op)
    assert result == 2, container.proc.stderr
    assert container.environment["KEEP"] == "1"
    assert container.environment["TOKEN"] == "secret"
    assert {"PIXI_AIRFLOW_INPUT", "PIXI_AIRFLOW_RUNNER"} <= set(container.environment)
    assert fake_pixi.calls[-1]["argv"][3:5] == ["--environment", "cuda"]
    assert op.environment == "cuda"
    assert op.env_vars == {"KEEP": "1"}
    assert op._private_environment == {"TOKEN": "secret"}
    assert op.entrypoint is None
    assert op.command is None


def test_the_variables_of_docker_operator_are_called_env_vars(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, tmp_path, env_vars="{{ params.env }}")
    with pytest.raises(TypeError, match="env_vars must be a dict of names and values, not str"):
        run(op)


def test_pickled_results_come_back(fake_pixi, tmp_path: Path) -> None:
    result, _ = run(make(fake_pixi, tmp_path, python_callable=when, op_args=[2026], serializer="pickle"))
    assert result == datetime.date(2026, 1, 2)


def test_an_exception_of_the_callable_fails_the_task_with_it(fake_pixi, tmp_path: Path) -> None:
    with pytest.raises(PixiCallableError, match="^boom raised ValueError: no rows in sales$") as e:
        run(make(fake_pixi, tmp_path, python_callable=boom, op_args=["sales"]))
    assert e.value.error_type == "ValueError"
    assert "in boom" in e.value.traceback


def test_other_failures_keep_the_error_of_docker_operator(make_fake_pixi, tmp_path: Path) -> None:
    with pytest.raises(DockerContainerFailedException) as e:
        run(make(make_fake_pixi("0.80.2"), tmp_path))
    assert f"pixi 0.80.2 in the image is older than {MIN_PIXI_VERSION}" in "\n".join(e.value.logs)


def test_a_function_that_does_not_return_fails(fake_pixi, tmp_path: Path) -> None:
    with pytest.raises(DockerContainerFailedException) as e:
        run(make(fake_pixi, tmp_path, python_callable="sys:exit", op_args=[0]))
    assert "exited without returning" in "\n".join(e.value.logs)


def test_skip_on_exit_code_skips(fake_pixi, tmp_path: Path) -> None:
    with pytest.raises(AirflowSkipException):
        run(make(fake_pixi, tmp_path, python_callable="sys:exit", op_args=[99], skip_on_exit_code=99))


def test_container_runs_in_a_project_of_the_image(fake_pixi, tmp_path: Path) -> None:
    workdir = tmp_path / "app"
    (workdir / "proj").mkdir(parents=True)
    (workdir / "proj" / "helpers.py").write_text("def triple(x):\n    return 3 * x\n")
    op = make(fake_pixi, tmp_path, pixi_project_path="proj", python_callable="helpers:triple", op_args=[4])
    result, container = run(op, workdir=workdir)
    assert result == 12, container.proc.stderr
    assert os.path.realpath(fake_pixi.calls[-1]["cwd"]) == os.path.realpath(workdir / "proj")


def test_too_large_arguments_fail_before_the_container_exists(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, tmp_path, op_args=["x" * MAX_ENV_VALUE_BYTES])
    with (
        patch.object(DockerOperator, "execute") as docker_execute,
        pytest.raises(AirflowException, match="PIXI_AIRFLOW_INPUT"),
    ):
        op.execute({})
    docker_execute.assert_not_called()


def test_context_parameters_reach_the_function(fake_pixi, tmp_path: Path) -> None:
    def step(x: int, ds=None):
        return [x, ds]

    result, _ = run(make(fake_pixi, tmp_path, python_callable=step, op_args=[1]), {"ds": "2026-01-02"})
    assert result == [1, "2026-01-02"]


def test_task_pixi_docker_ships_the_function_without_its_decorator(fake_pixi, tmp_path: Path) -> None:
    with DAG("d") as dag:

        @task.pixi_docker(pixi_binary=str(fake_pixi.path), docker_url="tcp://docker:2375", **INLINE)
        def double(x: int) -> int:
            return x * 2

        double(21)

    op = dag.get_task("double")
    assert type(op).__name__ == "PixiDockerDecoratedOperator"
    assert op.custom_operator_name == "@task.pixi_docker"
    assert op.docker_url == "tcp://docker:2375"
    assert "@task.pixi_docker" not in op.get_python_source()
    op.result_path = op.retrieve_output_path = str(tmp_path / "result")
    assert run(op)[0] == 42


def test_pixi_docker_task_is_the_registered_decorator() -> None:
    assert task.pixi_docker is pixi_docker_task


def test_an_exception_skips_when_skip_on_exit_code_has_its_exit_code(fake_pixi, tmp_path: Path) -> None:
    with pytest.raises(AirflowSkipException, match="exited with code 1"):
        run(make(fake_pixi, tmp_path, python_callable=boom, op_args=["sales"], skip_on_exit_code=1))


def test_a_result_that_cannot_be_copied_out_fails_the_task(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, tmp_path, python_callable=boom, op_args=["sales"])
    with (
        patch.object(DockerOperator, "execute", return_value=None),
        pytest.raises(AirflowException, match="could not be copied out"),
    ):
        op.execute({})


def test_a_function_returning_none_returns_none(fake_pixi, tmp_path: Path) -> None:
    assert run(make(fake_pixi, tmp_path, python_callable="builtins:print"))[0] is None


@pytest.mark.parametrize(
    "argument",
    [
        {"command": "echo"},
        {"entrypoint": ["/init"]},
        {"retrieve_output": False},
        {"retrieve_output_path": "/out"},
    ],
)
def test_arguments_the_operator_sets_are_rejected(argument) -> None:
    with pytest.raises(ValueError, match=f"sets {next(iter(argument))} itself"):
        PixiDockerOperator(task_id="t", python_callable=add, **INLINE, **argument)


def test_arguments_ending_in_dot_env_are_not_template_files(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, tmp_path, python_callable="builtins:str", op_args=["/run/secrets/app.env"])
    op.render_template_fields({})
    assert op.op_args == ["/run/secrets/app.env"]
    assert run(op)[0] == "/run/secrets/app.env"
