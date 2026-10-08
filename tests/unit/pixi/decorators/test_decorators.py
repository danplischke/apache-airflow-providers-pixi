from __future__ import annotations

import os
from unittest.mock import MagicMock

from airflow.sdk import dag, task


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
