"""``@task.pixi_branch``: choose the tasks to follow with a function run inside a Pixi environment."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from airflow.providers.pixi.decorators.pixi import PixiDecoratedOperator
from airflow.providers.pixi.operators.pixi import PixiBranchOperator
from airflow.sdk.bases.decorator import task_decorator_factory


class PixiBranchDecoratedOperator(PixiDecoratedOperator, PixiBranchOperator):  # type: ignore[misc]
    """``@task.pixi_branch``: choose the tasks to follow with a function run inside a Pixi environment."""

    custom_operator_name = "@task.pixi_branch"


def pixi_branch_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    **kwargs: Any,
):
    """``@task.pixi_branch``: choose the tasks to follow with a function run inside a Pixi environment.

    Like ``@task.branch_virtualenv``. The function returns a task id or task group id directly downstream, a
    list of them, or ``None`` to skip them all. Accepts every ``PixiBranchOperator`` argument; as for
    ``@task.pixi``, the function's source is shipped, so it must be self-contained.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=PixiBranchDecoratedOperator,
        **kwargs,
    )
