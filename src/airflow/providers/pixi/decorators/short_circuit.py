"""``@task.pixi_short_circuit``: continue only if a function run inside a Pixi environment returns a truthy value."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from airflow.sdk.bases.decorator import task_decorator_factory

from airflow.providers.pixi.decorators.pixi import PixiDecoratedOperator
from airflow.providers.pixi.operators.pixi import PixiShortCircuitOperator


class PixiShortCircuitDecoratedOperator(PixiDecoratedOperator, PixiShortCircuitOperator):  # type: ignore[misc]
    """``@task.pixi_short_circuit``: continue only if a function run inside a Pixi environment returns a truthy value."""

    custom_operator_name = "@task.pixi_short_circuit"


def pixi_short_circuit_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    **kwargs: Any,
):
    """``@task.pixi_short_circuit``: continue only if a function run inside a Pixi environment returns a truthy value.

    Like ``@task.short_circuit``. A falsy return value skips the tasks downstream. Accepts every
    ``PixiShortCircuitOperator`` argument, such as ``ignore_downstream_trigger_rules``; as for ``@task.pixi``, the
    function's source is shipped, so it must be self-contained.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=PixiShortCircuitDecoratedOperator,
        **kwargs,
    )
