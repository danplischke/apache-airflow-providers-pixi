"""Unit tests for the runner that runs inside the Pixi environment."""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

import pytest

from airflow.providers.pixi.runtime import runner
from airflow.providers.pixi.utils.source import RUNNER_SCRIPT


def describe(a, b=0, *, ds=None, params=None):
    return a, b, ds, params


def takes_everything(**kwargs):
    return kwargs


def test_the_shipped_source_is_this_module() -> None:
    assert Path(runner.__file__).read_text(encoding="utf-8") == RUNNER_SCRIPT


def test_the_runner_runs_on_python_3_10() -> None:
    ast.parse(RUNNER_SCRIPT, feature_version=(3, 10))


def test_the_runner_imports_only_the_standard_library() -> None:
    imported = set()
    for node in ast.walk(ast.parse(RUNNER_SCRIPT)):
        if isinstance(node, ast.Import):
            imported.update(alias.name.partition(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imported.add(node.module.partition(".")[0])
    assert imported - {"__future__"} <= sys.stdlib_module_names


@pytest.mark.parametrize(
    ("args", "kwargs", "expected"),
    [
        ([1], {}, {"ds": "2026-10-09", "params": {"x": 1}}),
        ([1], {"ds": "mine"}, {"ds": "mine", "params": {"x": 1}}),
        ([1, 2], {}, {"ds": "2026-10-09", "params": {"x": 1}}),
    ],
)
def test_context_fills_only_open_parameters(args, kwargs, expected) -> None:
    context = {"ds": "2026-10-09", "params": {"x": 1}, "run_id": "manual_1"}
    assert runner.with_context(describe, args, kwargs, context) == expected


def test_a_function_with_kwargs_gets_the_whole_context() -> None:
    context = {"ds": "2026-10-09", "run_id": "manual_1"}
    assert runner.with_context(takes_everything, [], {"run_id": "mine"}, context) == {
        "ds": "2026-10-09",
        "run_id": "mine",
    }


def test_a_callable_without_a_signature_gets_no_context() -> None:
    assert runner.with_context(print, [], {}, {"ds": "2026-10-09"}) == {}


def test_an_exception_is_described_with_its_module() -> None:
    try:
        json.loads("{")
    except ValueError as e:
        error = runner.describe_error(e)
    assert error["type"] == "json.decoder.JSONDecodeError"
    assert "Traceback (most recent call last)" in error["traceback"]


def test_a_builtin_exception_is_described_by_its_name() -> None:
    assert runner.describe_error(KeyError("k"))["type"] == "KeyError"


def test_a_result_json_cannot_hold_names_the_serializer() -> None:
    with pytest.raises(TypeError, match="pass serializer='pickle'"):
        runner.encode_result({1, 2}, "json", as_json=False)


def test_a_pickled_result_for_a_pod_is_a_base64_json_string() -> None:
    assert isinstance(json.loads(runner.encode_result([1], "pickle", as_json=True)), str)


def test_the_termination_message_keeps_the_end_of_a_long_traceback() -> None:
    error = {"type": "ValueError", "message": "m" * 1000, "traceback": "".join(f"line {i}\n" for i in range(2000))}
    data = runner.termination_message(error)
    assert len(data) <= runner.TERMINATION_MESSAGE_BYTES
    described = json.loads(data)[runner.TERMINATION_KEY]
    assert described["message"] == "m" * 500
    assert described["traceback"].endswith("line 1999\n")


@pytest.mark.parametrize(("version", "supported"), [((3, 9, 18), False), ((3, 10, 0), True), ((3, 14, 1), True)])
def test_the_runner_needs_python_3_10(version, supported) -> None:
    problem = runner.unsupported_python(version)
    assert (problem is None) is supported
    if not supported:
        assert problem == (
            "The Pixi environment has Python 3.9.18, but apache-airflow-providers-pixi needs Python 3.10 or newer."
        )


def test_a_too_old_python_fails_like_an_exception(tmp_path, monkeypatch) -> None:
    termination_log = tmp_path / "termination-log"
    monkeypatch.setenv(runner.TERMINATION_LOG_ENV, str(termination_log))
    monkeypatch.setattr(runner, "MIN_PYTHON", (99, 0))
    output = tmp_path / "output"
    assert runner.main(["-c", "json", str(tmp_path / "missing-input"), str(output)]) == 1
    error = json.loads(Path(f"{output}{runner.ERROR_SUFFIX}").read_text())
    assert error["type"] == "RuntimeError"
    assert error["message"].endswith("needs Python 99.0 or newer.")
    assert json.loads(termination_log.read_text())[runner.TERMINATION_KEY]["type"] == "RuntimeError"
    assert not output.exists()
