"""Unit tests for PixiOperator; a fake pixi runs the command with this interpreter."""

from __future__ import annotations

import datetime
import functools
import inspect
import os
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.sdk import DAG, dag, setup, task
from airflow.sdk.exceptions import AirflowTaskTimeout
from airflow.sdk.execution_time.timeout import timeout

from airflow.providers.pixi.operators.pixi import (
    PixiOperator,
    _build_pixi_toml,
    _ensure_pixi_available,
    _function_source,
)

INLINE = {"dependencies": {"python": "3.12.*"}}


def add(a, b=0):
    return a + b


def heartbeat(path):
    import time

    # bounded, so a run that is not stopped ends the test instead of hanging it
    end = time.monotonic() + 30
    while time.monotonic() < end:
        with open(path, "a") as f:
            f.write(".")
        time.sleep(0.05)


def make(fake_pixi, **kwargs) -> PixiOperator:
    if "pixi_project_path" not in kwargs and "pixi_toml_path" not in kwargs:
        kwargs.setdefault("dependencies", {"python": "3.12.*"})
    kwargs.setdefault("python_callable", add)
    return PixiOperator(task_id="t", pixi_binary=str(fake_pixi.path), auto_install_pixi=False, **kwargs)


def run(op: PixiOperator):
    return op.execute({"ti": MagicMock()})


def assert_stopped(beat: Path, started: float) -> None:
    assert time.monotonic() - started < 15, "the run was not stopped"
    assert beat.exists(), "the callable never started"
    size = beat.stat().st_size
    time.sleep(0.5)
    assert beat.stat().st_size == size, "the callable is still running"


def test_package_exports_operator() -> None:
    import airflow.providers.pixi as pixi_airflow

    assert pixi_airflow.PixiOperator is PixiOperator


@pytest.mark.parametrize("ref", ["nomodule", "a:b:c", ":f", "m:"])
def test_callable_string_must_be_module_and_name(ref: str) -> None:
    with pytest.raises(ValueError, match="module.path:callable_name"):
        PixiOperator(task_id="t", python_callable=ref, **INLINE)


@pytest.mark.parametrize("fn", [lambda: 1, len, str.upper])
def test_callable_must_be_def_function_or_string(fn) -> None:
    with pytest.raises(ValueError, match="defined with def"):
        PixiOperator(task_id="t", python_callable=fn, **INLINE)


def test_closure_variables_are_rejected() -> None:
    threshold = 5

    def above(x):
        return x > threshold

    with pytest.raises(ValueError, match="uses threshold from an enclosing function"):
        PixiOperator(task_id="t", python_callable=above, **INLINE)


def test_unknown_serializer_is_rejected() -> None:
    with pytest.raises(ValueError, match="serializer"):
        PixiOperator(task_id="t", python_callable=add, serializer="yaml", **INLINE)


def test_function_source_strips_task_decorators_and_keeps_line_numbers() -> None:
    @dag
    def test_dag():
        @setup
        @task.pixi(
            dependencies={"python": "3.12.*"},
        )
        def step(x):
            return x * 2

        step(1)

    source, filename = _function_source(test_dag().get_task("step").python_callable)
    assert filename == __file__
    assert "@setup" not in source and "@task.pixi" not in source and "dependencies" not in source
    file_lines = Path(__file__).read_text().splitlines()
    for number, line in enumerate(source.splitlines()):
        if line.strip():
            assert file_lines[number].strip() == line.strip()
    namespace: dict = {}
    exec(compile(source, filename, "exec"), namespace)  # noqa: S102 as the runner does
    assert namespace["step"](3) == 6


def test_function_source_of_a_wrapped_function_comes_from_its_file() -> None:
    # lru_cache's wrapper is not a function; its source and file are those of what it wraps
    source, filename = _function_source(functools.lru_cache(add))
    assert filename == __file__
    assert "def add(a, b=0):" in source


def test_execute_runs_the_function_source(fake_pixi) -> None:
    # add lives in this test module, which the environment cannot import
    assert run(make(fake_pixi, op_args=[2], op_kwargs={"b": 3})) == 5


def test_execute_imports_a_module_path(fake_pixi) -> None:
    assert run(make(fake_pixi, python_callable="json:dumps", op_args=[[1, 2]])) == "[1, 2]"


def test_result_is_returned_not_pushed(fake_pixi) -> None:
    ti = MagicMock()
    assert make(fake_pixi, op_args=[1]).execute({"ti": ti}) == 1
    ti.xcom_push.assert_not_called()


def test_output_is_streamed_to_the_task_log(fake_pixi) -> None:
    def chatty():
        import sys

        print("hello from pixi")
        print("to stderr", file=sys.stderr)
        return 1

    with patch.object(PixiOperator, "log", new_callable=PropertyMock) as log:
        assert run(make(fake_pixi, python_callable=chatty)) == 1
    logged = [c.args[1] for c in log.return_value.info.call_args_list if c.args[0] == "%s"]
    assert "hello from pixi" in logged
    assert "to stderr" in logged


def test_failure_raises_with_the_output_tail(fake_pixi) -> None:
    def boom():
        raise ValueError("kaputt")

    raise_line = inspect.getsourcelines(boom)[1] + 1
    with pytest.raises(AirflowException, match="exited with code 1") as excinfo:
        run(make(fake_pixi, python_callable=boom))
    assert "ValueError: kaputt" in str(excinfo.value)
    # the traceback points at the line in this file
    assert f'File "{__file__}", line {raise_line}, in boom' in str(excinfo.value)


def test_callable_exiting_early_fails(fake_pixi) -> None:
    def leave():
        import sys

        sys.exit(0)

    with pytest.raises(AirflowException, match="exited without returning"):
        run(make(fake_pixi, python_callable=leave))


def test_json_rejects_arguments_it_cannot_hold(fake_pixi) -> None:
    with pytest.raises(TypeError, match="serializer='pickle'"):
        run(make(fake_pixi, op_args=[datetime.date(2026, 1, 1)]))


def test_json_rejects_a_return_value_it_cannot_hold(fake_pixi) -> None:
    def today():
        import datetime

        return datetime.date(2026, 1, 1)

    with pytest.raises(AirflowException, match="serializer='pickle'"):
        run(make(fake_pixi, python_callable=today))


def test_pickle_serializer_round_trips(fake_pixi) -> None:
    def next_day(day):
        import datetime

        return day + datetime.timedelta(days=1)

    op = make(fake_pixi, python_callable=next_day, op_args=[datetime.date(2026, 1, 1)], serializer="pickle")
    assert run(op) == datetime.date(2026, 1, 2)


def test_op_args_xcom_adds_the_dependency() -> None:
    with DAG("d"):
        up = PixiOperator(task_id="up", python_callable=add, op_args=[1], **INLINE)
        down = PixiOperator(task_id="down", python_callable=add, op_args=[up.output], **INLINE)
    assert down.upstream_task_ids == {"up"}


def test_op_args_and_op_kwargs_are_templated() -> None:
    with DAG("d", params={"a": 1, "b": 2}, render_template_as_native_obj=True):
        op = PixiOperator(
            task_id="t", python_callable=add, op_args=["{{ params.a }}"], op_kwargs={"b": "{{ params.b }}"}, **INLINE
        )
    op.render_template_fields({"params": {"a": 1, "b": 2}})
    assert list(op.op_args) == [1]
    assert op.op_kwargs == {"b": 2}


def test_templated_project_path_is_used_at_run_time(fake_pixi, tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    with DAG("d"):
        op = make(fake_pixi, pixi_project_path="{{ params.project }}", python_callable="json:dumps", op_args=[1])
    op.render_template_fields({"params": {"project": str(project)}})
    run(op)
    call = fake_pixi.calls[-1]
    assert call["argv"][:3] == ["run", "--manifest-path", str(project)]
    assert os.path.realpath(call["cwd"]) == os.path.realpath(project)


def test_toml_path_is_passed_as_the_file(fake_pixi, tmp_path: Path) -> None:
    manifest = tmp_path / "pyproject.toml"
    manifest.write_text("")
    run(make(fake_pixi, pixi_toml_path=str(manifest), python_callable="json:dumps", op_args=[1]))
    call = fake_pixi.calls[-1]
    assert call["argv"][:3] == ["run", "--manifest-path", str(manifest)]
    assert os.path.realpath(call["cwd"]) == os.path.realpath(tmp_path)


def test_environment_is_passed(fake_pixi) -> None:
    run(make(fake_pixi, python_callable="json:dumps", op_args=[1], environment="cuda"))
    assert fake_pixi.calls[-1]["argv"][3:5] == ["--environment", "cuda"]


def test_cache_dir_variables_are_set_in_the_environment(fake_pixi) -> None:
    values = {"pixi_cache": "/shared/pixi", "uv_cache": "/shared/uv", "pip_cache": " /shared/pip\n"}
    op = make(
        fake_pixi,
        python_callable="json:dumps",
        op_args=[1],
        pixi_cache_dir_variable="pixi_cache",
        uv_cache_dir_variable="uv_cache",
        pip_cache_dir_variable="pip_cache",
    )
    with patch("airflow.providers.pixi.operators.pixi.Variable.get", side_effect=lambda key, default=None: values[key]):
        run(op)
    env = fake_pixi.calls[-1]["env"]
    assert env["PIXI_CACHE_DIR"] == "/shared/pixi"
    assert env["UV_CACHE_DIR"] == "/shared/uv"
    assert env["PIP_CACHE_DIR"] == "/shared/pip"
    assert env["PYTHONUNBUFFERED"]


def test_inline_manifest_directory_is_removed_after_the_run(fake_pixi) -> None:
    run(make(fake_pixi, python_callable="json:dumps", op_args=[1]))
    manifest = fake_pixi.calls[-1]["argv"][2]
    assert manifest.endswith("pixi.toml")
    assert not os.path.exists(os.path.dirname(manifest))


def test_env_cache_path_reuses_inline_environments(fake_pixi, tmp_path: Path) -> None:
    cache = tmp_path / "envs"
    for _ in range(2):
        run(make(fake_pixi, python_callable="json:dumps", op_args=[1], env_cache_path=str(cache)))
    run(
        make(
            fake_pixi,
            python_callable="json:dumps",
            op_args=[1],
            env_cache_path=str(cache),
            dependencies={"python": "3.11.*"},
        )
    )
    manifests = [call["argv"][2] for call in fake_pixi.calls]
    assert manifests[0] == manifests[1] != manifests[2]
    assert all(os.path.exists(manifest) for manifest in manifests)
    assert {path.name for path in cache.iterdir()} == {Path(manifest).parent.name for manifest in manifests}


def test_on_kill_stops_the_run(fake_pixi, tmp_path: Path) -> None:
    beat = tmp_path / "beat"
    op = make(fake_pixi, python_callable=heartbeat, op_args=[str(beat)])

    def kill_once_beating():
        deadline = time.monotonic() + 30
        while not beat.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        op.on_kill()

    threading.Thread(target=kill_once_beating, daemon=True).start()
    started = time.monotonic()
    with pytest.raises(AirflowException, match="exited with code"):
        run(op)
    assert_stopped(beat, started)


def test_execution_timeout_stops_the_run(fake_pixi, tmp_path: Path) -> None:
    beat = tmp_path / "beat"
    op = make(fake_pixi, python_callable=heartbeat, op_args=[str(beat)])
    started = time.monotonic()
    with pytest.raises(AirflowTaskTimeout), timeout(3):
        run(op)
    assert_stopped(beat, started)


def test_build_pixi_toml_minimal() -> None:
    toml = _build_pixi_toml(
        channels=["conda-forge"],
        platforms=["linux-64"],
    )
    assert "[workspace]" in toml
    assert 'channels = ["conda-forge"]' in toml
    assert 'platforms = ["linux-64"]' in toml


def test_build_pixi_toml_with_dependencies() -> None:
    toml = _build_pixi_toml(
        channels=["conda-forge"],
        platforms=["linux-64", "osx-64"],
        dependencies={"python": ">=3.10", "numpy": "*"},
    )
    assert "[dependencies]" in toml
    assert "python" in toml
    assert "numpy" in toml


def test_build_pixi_toml_with_pypi() -> None:
    toml = _build_pixi_toml(
        channels=["conda-forge"],
        platforms=["linux-64"],
        pypi_dependencies={"pandas": ">=2.0"},
    )
    assert "[pypi-dependencies]" in toml
    assert "pandas" in toml


def test_build_pixi_toml_with_environments() -> None:
    toml = _build_pixi_toml(
        channels=["conda-forge"],
        platforms=["linux-64"],
        environments={"test": ["test"], "cuda": ["cuda"]},
    )
    assert "[environments]" in toml
    assert "test" in toml
    assert "cuda" in toml


def test_operator_init_requires_one_manifest_source() -> None:
    with pytest.raises(ValueError, match="Exactly one of"):
        PixiOperator(
            task_id="t",
            pixi_project_path="/tmp",
            pixi_toml_path="/tmp/pixi.toml",
            python_callable="m:f",
        )
    with pytest.raises(ValueError, match="Exactly one of"):
        PixiOperator(
            task_id="t",
            python_callable="m:f",
        )
    with pytest.raises(ValueError, match="Exactly one of"):
        PixiOperator(
            task_id="t",
            pixi_project_path="/tmp",
            dependencies={"python": "3.10"},
            python_callable="m:f",
        )


def test_ensure_pixi_available_uses_which_when_found() -> None:
    """When pixi is on PATH, _ensure_pixi_available returns it without installing."""
    with patch("airflow.providers.pixi.operators.pixi.shutil.which") as m_which:
        m_which.return_value = "/usr/local/bin/pixi"
        path = _ensure_pixi_available("pixi", auto_install=True)
    assert path == "/usr/local/bin/pixi"
    m_which.assert_called_once_with("pixi")


def test_ensure_pixi_available_raises_when_not_found_and_no_auto_install() -> None:
    """When pixi not on PATH and auto_install=False, raises AirflowException."""
    with (
        patch("airflow.providers.pixi.operators.pixi.shutil.which", return_value=None),
        pytest.raises(AirflowException, match="not found on PATH"),
    ):
        _ensure_pixi_available("pixi", auto_install=False)
