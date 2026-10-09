"""Unit tests for PixiBranchOperator and @task.pixi_branch; a fake pixi runs the callable with this interpreter.

The skips are checked where Airflow's task runner takes them over: SkipMixin raises DownstreamTasksSkipped with
the tasks to skip, and pushes the followed tasks as an XCom.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, TaskGroup

from airflow.providers.pixi.decorators.branch import PixiBranchDecoratedOperator, pixi_branch_task
from airflow.providers.pixi.operators.pixi import BasePixiPythonOperator, PixiBranchOperator, PixiOperator
from airflow.providers.pixi.utils.compat import AirflowException, BranchMixIn

INLINE = {"dependencies": {"python": "3.12.*"}}


def choose(branch):
    return branch


def choose_from_params(params):
    return params["branch"]


def branch_dag(fake_pixi, **kwargs: Any) -> tuple[DAG, PixiBranchOperator]:
    """``branch >> [a, b, c]``."""
    kwargs.setdefault("python_callable", choose)
    with DAG("branch") as dag:
        op = PixiBranchOperator(task_id="branch", pixi_binary=str(fake_pixi.path), **INLINE, **kwargs)
        op >> [EmptyOperator(task_id=task_id) for task_id in ("a", "b", "c")]
    return dag, op


def execute(op, **context: Any) -> tuple[Any, list[str] | None, MagicMock]:
    """Run ``op``; return its result, the task ids it skips (``None`` if none) and the task instance."""
    ti = MagicMock(task=op, map_index=-1)
    try:
        result = op.execute({"ti": ti, "task": op, **context})
    except AirflowException as e:
        if type(e).__name__ != "DownstreamTasksSkipped":
            raise
        return None, sorted(task_id for task_id, _ in e.tasks), ti
    return result, None, ti


def followed(ti: MagicMock) -> list[str]:
    (call,) = [c for c in ti.xcom_push.call_args_list if c.kwargs["key"] == "skipmixin_key"]
    return sorted(call.kwargs["value"]["followed"])


def test_branch_operator_is_a_pixi_operator_that_branches() -> None:
    assert issubclass(PixiBranchOperator, PixiOperator)
    assert issubclass(PixiBranchOperator, BranchMixIn)
    assert PixiBranchOperator.inherits_from_skipmixin is True


def test_one_task_id_skips_the_other_branches(fake_pixi) -> None:
    _, op = branch_dag(fake_pixi, op_args=["a"])
    _, skipped, ti = execute(op)
    assert skipped == ["b", "c"]
    assert followed(ti) == ["a"]
    assert len(fake_pixi.calls) == 1


def test_a_list_of_task_ids_skips_the_rest(fake_pixi) -> None:
    _, op = branch_dag(fake_pixi, op_args=[["a", "c"]])
    _, skipped, ti = execute(op)
    assert skipped == ["b"]
    assert followed(ti) == ["a", "c"]


def test_none_skips_every_branch(fake_pixi) -> None:
    _, op = branch_dag(fake_pixi, op_args=[None])
    _, skipped, ti = execute(op)
    assert skipped == ["a", "b", "c"]
    assert followed(ti) == []


def test_following_every_branch_skips_nothing_and_returns_the_choice(fake_pixi) -> None:
    _, op = branch_dag(fake_pixi, op_args=[["a", "b", "c"]])
    result, skipped, _ = execute(op)
    assert skipped is None
    assert result == ["a", "b", "c"]


def test_a_task_group_id_follows_the_groups_roots(fake_pixi) -> None:
    with DAG("groups"):
        op = PixiBranchOperator(
            task_id="branch", python_callable=choose, op_args=["group"], pixi_binary=str(fake_pixi.path), **INLINE
        )
        with TaskGroup("group") as group:
            EmptyOperator(task_id="first") >> EmptyOperator(task_id="second")
        op >> [group, EmptyOperator(task_id="other")]
    _, skipped, ti = execute(op)
    assert skipped == ["other"]
    assert followed(ti) == ["group.first"]


def test_a_task_after_the_followed_branch_is_not_skipped(fake_pixi) -> None:
    with DAG("join"):
        op = PixiBranchOperator(
            task_id="branch", python_callable=choose, op_args=["a"], pixi_binary=str(fake_pixi.path), **INLINE
        )
        a, b, join = (EmptyOperator(task_id=task_id) for task_id in ("a", "b", "join"))
        op >> [a, b, join]
        a >> join
    _, skipped, _ = execute(op)
    assert skipped == ["b"]


def test_context_parameters_reach_the_callable(fake_pixi) -> None:
    _, op = branch_dag(fake_pixi, python_callable=choose_from_params)
    _, skipped, _ = execute(op, params={"branch": "b"})
    assert skipped == ["a", "c"]


def test_a_task_id_not_in_the_dag_fails(fake_pixi) -> None:
    _, op = branch_dag(fake_pixi, op_args=["missing"])
    with pytest.raises(AirflowException, match="must contain only valid task_ids"):
        execute(op)


def test_a_return_value_that_is_no_task_id_fails(fake_pixi) -> None:
    _, op = branch_dag(fake_pixi, op_args=[1])
    with pytest.raises(AirflowException, match="branch_task_ids"):
        execute(op)


def test_pixi_branch_task_builds_the_branch_operator(fake_pixi) -> None:
    with DAG("decorated") as dag:

        @pixi_branch_task(pixi_binary=str(fake_pixi.path), **INLINE)
        def pick(branch: str) -> str:
            return branch

        pick("b") >> [EmptyOperator(task_id="a"), EmptyOperator(task_id="b")]

    op = dag.get_task("pick")
    assert type(op) is PixiBranchDecoratedOperator
    assert isinstance(op, PixiBranchOperator)
    assert op.custom_operator_name == "@task.pixi_branch"
    assert op.downstream_task_ids == {"a", "b"}
    assert {"op_args", "op_kwargs", "pixi_project_path"} <= set(op.template_fields)
    assert "pixi_branch_task" not in op.get_python_source()
    _, skipped, _ = execute(op)
    assert skipped == ["a"]


def test_task_pixi_branch_is_stripped_and_keeps_the_line_numbers(fake_pixi) -> None:
    task = SimpleNamespace(pixi_branch=pixi_branch_task)
    with DAG("lines") as dag:

        @task.pixi_branch(pixi_binary=str(fake_pixi.path), **INLINE)
        def fails_on_its_line() -> None:
            raise KeyError("line marker of the branch function")

        fails_on_its_line()

    op = dag.get_task("fails_on_its_line")
    source = op.get_python_source()
    assert "task.pixi_branch" not in source
    with open(__file__) as f:
        line = next(i for i, text in enumerate(f, 1) if 'raise KeyError("line marker of the branch' in text)
    assert "line marker" in source.splitlines()[line - 1]
    assert PixiBranchDecoratedOperator.get_python_source is BasePixiPythonOperator.get_python_source
