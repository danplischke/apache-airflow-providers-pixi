"""Unit tests for PixiShortCircuitOperator and @task.pixi_short_circuit; a fake pixi runs the callable.

As in test_branch.py, the skips are checked where Airflow's task runner takes them over: SkipMixin raises
DownstreamTasksSkipped with the tasks to skip.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from airflow.providers.pixi.decorators.short_circuit import (
    PixiShortCircuitDecoratedOperator,
    pixi_short_circuit_task,
)
from airflow.providers.pixi.operators.pixi import BasePixiPythonOperator, PixiOperator, PixiShortCircuitOperator
from airflow.providers.pixi.utils.compat import AirflowException, SkipMixin

INLINE = {"dependencies": {"python": "3.12.*"}}


def condition(value):
    return value


def condition_from_params(params):
    return params["go"]


def short_circuit_dag(fake_pixi, **kwargs: Any) -> PixiShortCircuitOperator:
    """``check >> direct >> after >> cleanup``, with ``cleanup`` a teardown task, and ``check >> other``."""
    kwargs.setdefault("python_callable", condition)
    with DAG("short_circuit"):
        op = PixiShortCircuitOperator(task_id="check", pixi_binary=str(fake_pixi.path), **INLINE, **kwargs)
        direct, after, other = (EmptyOperator(task_id=task_id) for task_id in ("direct", "after", "other"))
        cleanup = EmptyOperator(task_id="cleanup").as_teardown()
        op >> direct >> after >> cleanup
        op >> other
    return op


def execute(op, **context: Any) -> tuple[Any, list[str] | None, MagicMock]:
    """Run ``op``; return its result, the task ids it skips (``None`` if none) and the task instance."""
    ti = MagicMock(task=op, map_index=-1)
    try:
        result = op.execute({"ti": ti, "task": op, **context})
    except AirflowException as e:
        if type(e).__name__ != "DownstreamTasksSkipped":
            raise
        return None, sorted(e.tasks), ti
    return result, None, ti


def pushed_skips(ti: MagicMock) -> list[str]:
    """The skipped task ids of the XCom that NotPreviouslySkippedDep reads when they are cleared."""
    (call,) = ti.xcom_push.call_args_list
    assert call.kwargs["key"] == "skipmixin_key"
    return sorted(call.kwargs["value"]["skipped"])


def test_short_circuit_operator_is_a_pixi_operator_that_skips() -> None:
    assert issubclass(PixiShortCircuitOperator, PixiOperator)
    assert issubclass(PixiShortCircuitOperator, SkipMixin)
    assert PixiShortCircuitOperator.inherits_from_skipmixin is True


@pytest.mark.parametrize("value", [True, 1, "yes", {"rows": 3}])
def test_a_truthy_condition_skips_nothing_and_is_the_xcom(fake_pixi, value) -> None:
    op = short_circuit_dag(fake_pixi, op_args=[value])
    result, skipped, ti = execute(op)
    assert skipped is None
    assert result == value
    ti.xcom_push.assert_not_called()
    assert len(fake_pixi.calls) == 1


@pytest.mark.parametrize("value", [False, 0, None, []])
def test_a_falsy_condition_skips_everything_downstream_but_teardowns(fake_pixi, value) -> None:
    op = short_circuit_dag(fake_pixi, op_args=[value])
    _, skipped, ti = execute(op)
    assert skipped == ["after", "direct", "other"]
    assert pushed_skips(ti) == skipped


def test_without_ignoring_trigger_rules_only_the_direct_downstream_tasks_are_skipped(fake_pixi) -> None:
    op = short_circuit_dag(fake_pixi, op_args=[False], ignore_downstream_trigger_rules=False)
    assert op.ignore_downstream_trigger_rules is False
    _, skipped, _ = execute(op)
    assert skipped == ["direct", "other"]


def test_ignoring_trigger_rules_is_the_default(fake_pixi) -> None:
    assert short_circuit_dag(fake_pixi).ignore_downstream_trigger_rules is True


def test_a_falsy_condition_without_downstream_tasks_is_returned(fake_pixi) -> None:
    with DAG("alone"):
        op = PixiShortCircuitOperator(
            task_id="check", python_callable=condition, op_args=[0], pixi_binary=str(fake_pixi.path), **INLINE
        )
    result, skipped, ti = execute(op)
    assert (result, skipped) == (0, None)
    ti.xcom_push.assert_not_called()


def test_a_mapped_short_circuit_leaves_the_skips_to_the_scheduler(fake_pixi) -> None:
    op = short_circuit_dag(fake_pixi, op_args=[False])
    ti = MagicMock(task=op, map_index=0)
    assert op.execute({"ti": ti, "task": op}) is False
    assert pushed_skips(ti) == ["after", "direct", "other"]


def test_context_parameters_reach_the_condition(fake_pixi) -> None:
    op = short_circuit_dag(fake_pixi, python_callable=condition_from_params)
    assert execute(op, params={"go": True})[:2] == (True, None)
    assert execute(op, params={"go": False})[1] == ["after", "direct", "other"]


def test_pixi_short_circuit_task_builds_the_short_circuit_operator(fake_pixi) -> None:
    with DAG("decorated") as dag:

        @pixi_short_circuit_task(pixi_binary=str(fake_pixi.path), ignore_downstream_trigger_rules=False, **INLINE)
        def go(value: bool) -> bool:
            return value

        go(False) >> EmptyOperator(task_id="direct") >> EmptyOperator(task_id="after")

    op = dag.get_task("go")
    assert type(op) is PixiShortCircuitDecoratedOperator
    assert isinstance(op, PixiShortCircuitOperator)
    assert op.custom_operator_name == "@task.pixi_short_circuit"
    assert op.ignore_downstream_trigger_rules is False
    assert {"op_args", "op_kwargs", "pixi_project_path"} <= set(op.template_fields)
    assert "pixi_short_circuit_task" not in op.get_python_source()
    _, skipped, _ = execute(op)
    assert skipped == ["direct"]


def test_task_pixi_short_circuit_is_stripped_and_keeps_the_line_numbers(fake_pixi) -> None:
    task = SimpleNamespace(pixi_short_circuit=pixi_short_circuit_task)
    with DAG("lines") as dag:

        @task.pixi_short_circuit(pixi_binary=str(fake_pixi.path), **INLINE)
        def fails_on_its_line() -> None:
            raise KeyError("line marker of the short circuit function")

        fails_on_its_line()

    op = dag.get_task("fails_on_its_line")
    source = op.get_python_source()
    assert "task.pixi_short_circuit" not in source
    with open(__file__) as f:
        line = next(i for i, text in enumerate(f, 1) if 'raise KeyError("line marker of the short' in text)
    assert "line marker" in source.splitlines()[line - 1]
    assert PixiShortCircuitDecoratedOperator.get_python_source is BasePixiPythonOperator.get_python_source
