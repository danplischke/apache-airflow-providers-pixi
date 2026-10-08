"""Unit tests for PixiOperator; a fake pixi runs the command with this interpreter."""

from __future__ import annotations

import datetime
import functools
import inspect
import os
import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.sdk import DAG, dag, setup, task
from airflow.sdk.exceptions import AirflowTaskTimeout
from airflow.sdk.execution_time.timeout import timeout

from airflow.providers.pixi.operators.pixi import PixiOperator
from airflow.providers.pixi.utils.manifest import build_pixi_toml as _build_pixi_toml
from airflow.providers.pixi.utils.manifest import (
    pypi_dependencies_from_requirements as _pypi_dependencies_from_requirements,
)
from airflow.providers.pixi.utils.pixi import MIN_PIXI_VERSION
from airflow.providers.pixi.utils.pixi import resolve_pixi as _resolve_pixi
from airflow.providers.pixi.utils.source import function_source as _function_source

if sys.version_info >= (3, 11):
    import tomllib
else:
    tomllib = pytest.importorskip("tomli")

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
    return PixiOperator(task_id="t", pixi_binary=str(fake_pixi.path), **kwargs)


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
    assert parse_toml(toml) == {"workspace": {"channels": ["conda-forge"], "platforms": ["linux-64"]}}


def test_build_pixi_toml_with_dependencies() -> None:
    toml = _build_pixi_toml(
        channels=["conda-forge"],
        platforms=["linux-64", "osx-64"],
        dependencies={"python": ">=3.10", "numpy": "*"},
    )
    assert parse_toml(toml)["dependencies"] == {"python": ">=3.10", "numpy": "*"}


def test_build_pixi_toml_with_pypi() -> None:
    toml = _build_pixi_toml(
        channels=["conda-forge"],
        platforms=["linux-64"],
        pypi_dependencies={"pandas": ">=2.0"},
    )
    assert parse_toml(toml)["pypi-dependencies"] == {"pandas": ">=2.0"}


def test_build_pixi_toml_with_environments() -> None:
    toml = _build_pixi_toml(
        channels=["conda-forge"],
        platforms=["linux-64"],
        environments={"test": ["test"], "cuda": ["cuda"]},
    )
    assert parse_toml(toml)["environments"] == {"test": ["test"], "cuda": ["cuda"]}


def parse_toml(text: str) -> dict:
    return tomllib.loads(text)


def test_build_pixi_toml_writes_valid_toml_for_tables_and_quotes() -> None:
    toml = _build_pixi_toml(
        channels=["conda-forge"],
        platforms=["linux-64"],
        workspace_name='my "project"',
        dependencies=["python 3.12.*", "numpy>=2", "conda-forge::scipy"],
        pypi_dependencies={"torch": {"version": ">=2", "extras": ["cuda"]}, "pandas": ">=2.0"},
        pypi_options={"index-url": "https://pypi.example/simple"},
        feature={"gpu": {"platforms": ["linux-64"], "dependencies": {"cuda": "12.*"}}},
        environments={"gpu": {"features": ["gpu"], "solve-group": "default"}},
    )
    manifest = parse_toml(toml)
    assert manifest["workspace"]["name"] == 'my "project"'
    assert manifest["dependencies"] == {
        "python": "3.12.*",
        "numpy": ">=2",
        "scipy": {"version": "*", "channel": "conda-forge"},
    }
    assert manifest["pypi-dependencies"]["torch"] == {"version": ">=2", "extras": ["cuda"]}
    assert manifest["pypi-options"]["index-url"] == "https://pypi.example/simple"
    assert manifest["feature"]["gpu"]["dependencies"] == {"cuda": "12.*"}
    assert manifest["environments"]["gpu"] == {"features": ["gpu"], "solve-group": "default"}


def test_requirements_become_pypi_dependencies() -> None:
    requirements_file = """
        # comment
        requests>=2.31  # trailing comment

        black[jupyter]==24.1
    """
    assert _pypi_dependencies_from_requirements(
        [
            "pandas",
            requirements_file,
            "mylib @ git+https://github.com/org/mylib@v1.2",
            "private @ git+ssh://git@github.com/org/private",
            "wheel-pkg @ https://example.com/wheel_pkg-1.0-py3-none-any.whl",
        ]
    ) == {
        "pandas": "*",
        "requests": ">=2.31",
        "black": {"version": "==24.1", "extras": ["jupyter"]},
        "mylib": {"git": "https://github.com/org/mylib", "rev": "v1.2"},
        "private": {"git": "ssh://git@github.com/org/private"},
        "wheel-pkg": {"url": "https://example.com/wheel_pkg-1.0-py3-none-any.whl"},
    }


@pytest.mark.parametrize(
    ("requirements", "error"),
    [
        (["-r other.txt"], "pip options"),
        (['pandas; python_version < "3.11"'], "environment markers"),
        (["pandas", "Pandas>=2"], "listed twice"),
        (["not a requirement!"], "invalid requirement"),
    ],
)
def test_unsupported_requirements_are_rejected_when_the_dag_is_parsed(requirements, error) -> None:
    with pytest.raises(ValueError, match=error):
        PixiOperator(task_id="t", python_callable=add, requirements=requirements)


def test_requirements_alone_make_an_inline_manifest_with_the_workers_python(fake_pixi) -> None:
    run(
        make(fake_pixi, dependencies=None, requirements="pandas>=2\nrequests", op_args=[1], cleanup_temp_manifest=False)
    )
    manifest = parse_toml(Path(fake_pixi.calls[-1]["argv"][2]).read_text())
    assert manifest["dependencies"] == {"python": f"{sys.version_info.major}.{sys.version_info.minor}.*"}
    assert manifest["pypi-dependencies"] == {"pandas": ">=2", "requests": "*"}


def test_requirements_are_templated(fake_pixi) -> None:
    op = make(fake_pixi, requirements=["{{ params.package }}"], dependencies=None)
    assert "requirements" in op.template_fields
    op.render_template_fields({"params": {"package": "pandas==2.2"}})
    assert parse_toml(op.inline_manifest_toml())["pypi-dependencies"] == {"pandas": "==2.2"}


def test_requirements_and_pypi_dependencies_must_not_overlap(fake_pixi) -> None:
    op = make(fake_pixi, pypi_dependencies={"Pandas": "*"}, requirements=["pandas"])
    with pytest.raises(ValueError, match="both pypi_dependencies and requirements"):
        run(op)


def test_requirements_cannot_extend_a_project(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Exactly one of"):
        PixiOperator(task_id="t", python_callable=add, pixi_project_path=str(tmp_path), requirements=["pandas"])


def test_env_vars_reach_the_run_and_override_the_workers(fake_pixi, monkeypatch) -> None:
    monkeypatch.setenv("PIXI_CACHE_DIR", "/worker")
    op = make(
        fake_pixi,
        python_callable="os:getenv",
        op_args=["MY_SECRET"],
        env_vars={"MY_SECRET": "{{ not rendered }}", "PIXI_CACHE_DIR": "/task"},
    )
    assert "env_vars" not in op.template_fields
    assert run(op) == "{{ not rendered }}"
    assert fake_pixi.calls[-1]["env"]["PIXI_CACHE_DIR"] == "/task"


def test_get_python_source_is_what_runs(fake_pixi) -> None:
    class Negating(PixiOperator):
        def get_python_source(self) -> str:
            return (
                super().get_python_source()
                + "\n_add = add\n\ndef add(*args, **kwargs):\n    return -_add(*args, **kwargs)\n"
            )

    op = Negating(task_id="t", python_callable=add, op_args=[2, 3], pixi_binary=str(fake_pixi.path), **INLINE)
    assert run(op) == -5


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


def test_missing_pixi_fails_without_installing_it(monkeypatch) -> None:
    monkeypatch.setenv("PATH", "")
    with (
        patch("airflow.providers.pixi.utils.pixi.subprocess.run") as subprocess_run,
        pytest.raises(AirflowException, match=f"not found on PATH. Install pixi {MIN_PIXI_VERSION} or newer"),
    ):
        _resolve_pixi("pixi")
    subprocess_run.assert_not_called()


def test_pixi_older_than_the_minimum_is_rejected(make_fake_pixi) -> None:
    old = make_fake_pixi("0.80.2")
    op = PixiOperator(task_id="t", python_callable="json:dumps", op_args=[1], pixi_binary=str(old.path), **INLINE)
    with pytest.raises(AirflowException, match=f"is pixi 0.80.2, but this provider needs pixi {MIN_PIXI_VERSION}"):
        run(op)
    assert old.calls == []


@pytest.mark.parametrize("version", ["0.81.0", "0.81.1", "1.2.0", "0.90.0-dev"])
def test_pixi_at_or_above_the_minimum_runs(make_fake_pixi, version: str) -> None:
    pixi = make_fake_pixi(version)
    assert (
        run(PixiOperator(task_id="t", python_callable="json:dumps", op_args=[1], pixi_binary=str(pixi.path), **INLINE))
        == "1"
    )


def test_pixi_version_is_asked_once_per_binary(fake_pixi) -> None:
    run(make(fake_pixi, op_args=[1]))
    run(make(fake_pixi, op_args=[2]))
    assert fake_pixi.version_calls == 1


def test_unreadable_pixi_version_fails(tmp_path: Path) -> None:
    binary = tmp_path / "pixi"
    binary.write_text("#!/bin/sh\necho 'not pixi'\n")
    binary.chmod(0o755)
    with pytest.raises(AirflowException, match="Could not read the pixi version.*not pixi"):
        _resolve_pixi(str(binary))


def test_ci_tests_against_the_minimum_pixi() -> None:
    workflow = (Path(__file__).parents[4] / ".github" / "workflows" / "qa.yml").read_text()
    assert f"PIXI_VERSION: v{MIN_PIXI_VERSION}" in workflow
