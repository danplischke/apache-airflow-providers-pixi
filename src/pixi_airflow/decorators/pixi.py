"""``@task.pixi``: run the function inside a Pixi environment."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from airflow.sdk.bases.decorator import DecoratedOperator, task_decorator_factory

from pixi_airflow.operators.pixi import PixiOperator


class PixiDecoratedOperator(DecoratedOperator, PixiOperator):  # type: ignore[misc]
    """``@task.pixi``: run the function inside a Pixi environment."""

    custom_operator_name = "@task.pixi"
    # PixiOperator's, which include DecoratedOperator's op_args and op_kwargs
    template_fields = PixiOperator.template_fields
    template_fields_renderers = PixiOperator.template_fields_renderers

    def __init__(
        self,
        *,
        python_callable: Callable[..., Any],
        op_args: Sequence[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        # DecoratedOperator sets op_args/op_kwargs itself, then calls the upstream
        # __init__ with kwargs_to_upstream; PixiOperator must see the same values
        # or it resets them and XComArg dependencies are lost.
        super().__init__(
            kwargs_to_upstream={
                "python_callable": python_callable,
                "op_args": op_args,
                "op_kwargs": op_kwargs,
            },
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            **kwargs,
        )


def pixi_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    **kwargs: Any,
):
    """``@task.pixi``: run the function inside a Pixi environment.

    Accepts every ``PixiOperator`` argument. Like ``@task.virtualenv``, the function's source is
    shipped to the environment, so it must be self-contained: imports go inside the function.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=PixiDecoratedOperator,
        **kwargs,
    )
