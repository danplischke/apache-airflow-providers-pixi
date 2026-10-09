from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from airflow.sdk import DAG, dag, task

from airflow.providers.pixi.decorators.pixi import PixiDecoratedOperator, pixi_task
from airflow.providers.pixi.exceptions import PixiCallableError
from airflow.providers.pixi.operators.pixi import BasePixiPythonOperator

INLINE = {"dependencies": {"python": "3.12.*"}}


def add_one(x: int) -> int:
    return x + 1


def test_task_decorator_registered() -> None:
    assert hasattr(task, "pixi")


def test_task_pixi_builds_operator() -> None:
    @dag
    def test_dag():
        task.pixi(pixi_project_path="/proj", environment="cuda")(add_one)(2)

    op = test_dag().get_task("add_one")
    assert type(op).__name__ == "PixiDecoratedOperator"
    assert op.custom_operator_name == "@task.pixi"
    assert op.python_callable is add_one
    assert op.pixi_project_path == "/proj"
    assert op.environment == "cuda"
    assert list(op.op_args) == [2]
    assert {"op_args", "op_kwargs", "pixi_project_path"} <= set(op.template_fields)
    assert len(op.template_fields) == len(set(op.template_fields))


def test_task_pixi_keeps_xcom_args_and_dependencies() -> None:
    """Regression: PixiOperator must not reset op_args set by the TaskFlow decorator."""

    @dag
    def test_dag():
        @task
        def produce():
            return 1

        task.pixi(pixi_project_path="/proj")(add_one)(produce())

    consume_op = test_dag().get_task("add_one")
    assert consume_op.upstream_task_ids == {"produce"}
    assert len(consume_op.op_args) == 1


def test_task_pixi_runs_a_function_defined_in_the_dag(fake_pixi, tmp_path) -> None:
    """The function only exists inside the DAG function, so it has to travel as source."""

    @dag
    def test_dag():
        @task.pixi(pixi_project_path=str(tmp_path), pixi_binary=str(fake_pixi.path))
        def double(x: int) -> dict:
            import sys

            return {"value": x * 2, "prefix": sys.prefix}

        double(21)

    op = test_dag().get_task("double")
    result = op.execute({"ti": MagicMock()})
    assert result["value"] == 42
    call = fake_pixi.calls[-1]
    assert call["argv"][:3] == ["run", "--manifest-path", str(tmp_path)]
    assert os.path.realpath(call["cwd"]) == os.path.realpath(tmp_path)


def test_task_pixi_resolves_a_relative_project_against_the_dag_file(fake_pixi, tmp_path, monkeypatch) -> None:
    project = os.path.relpath(tmp_path, os.path.dirname(__file__))
    monkeypatch.chdir("/")

    @dag
    def test_dag():
        @task.pixi(pixi_project_path=project, lock_mode="frozen", pixi_binary=str(fake_pixi.path))
        def one() -> int:
            return 1

        one()

    assert test_dag().get_task("one").execute({"ti": MagicMock()}) == 1
    argv = fake_pixi.calls[-1]["argv"]
    assert os.path.realpath(argv[2]) == os.path.realpath(tmp_path)
    assert argv[3] == "--frozen"


def test_task_pixi_reports_the_exception_of_the_function(fake_pixi) -> None:
    @dag
    def test_dag():
        @task.pixi(dependencies={"python": "3.12.*"}, pixi_binary=str(fake_pixi.path))
        def fails() -> None:
            raise KeyError("missing")

        fails()

    with pytest.raises(PixiCallableError, match="fails raised KeyError: 'missing'"):
        test_dag().get_task("fails").execute({"ti": MagicMock()})


def line_in_this_file(marker: str) -> int:
    """Return the number of the one line of this file that is ``marker``, apart from its indentation."""
    (number,) = [i for i, line in enumerate(Path(__file__).read_text().splitlines(), 1) if line.strip() == marker]
    return number


def test_task_pixi_ships_the_source_of_pixi_operator() -> None:
    assert PixiDecoratedOperator.get_python_source is BasePixiPythonOperator.get_python_source


def test_task_pixi_tracebacks_point_at_the_dag_file(fake_pixi) -> None:
    with DAG("lines") as dag:

        @task.pixi(pixi_binary=str(fake_pixi.path), **INLINE)
        def fails_on_its_line() -> None:
            # a comment, which must keep its line too
            raise KeyError("line marker of the decorated function")

        fails_on_its_line()

    op = dag.get_task("fails_on_its_line")
    line = line_in_this_file('raise KeyError("line marker of the decorated function")')
    assert "line marker" in op.get_python_source().splitlines()[line - 1]
    with pytest.raises(PixiCallableError) as excinfo:
        op.execute({"ti": MagicMock()})
    assert f'File "{__file__}", line {line}, in fails_on_its_line' in excinfo.value.traceback


def test_pixi_task_imported_under_its_own_name_is_stripped(fake_pixi) -> None:
    with DAG("aliased") as dag:

        @pixi_task(pixi_binary=str(fake_pixi.path), **INLINE)
        def aliased(x: int) -> int:
            return x + 1

        aliased(1)

    op = dag.get_task("aliased")
    assert "pixi_task" not in op.get_python_source()
    assert op.execute({"ti": MagicMock()}) == 2
