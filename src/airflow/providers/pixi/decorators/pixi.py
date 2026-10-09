"""``@task.pixi``: run the function inside a Pixi environment."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from airflow.providers.pixi.operators.pixi import BasePixiPythonOperator, PixiOperator
from airflow.sdk.bases.decorator import DecoratedOperator, task_decorator_factory


class BasePixiDecoratedOperator(DecoratedOperator):
    """The TaskFlow half of a decorator that ships the function to a Pixi environment.

    Combine it with an operator built on
    [`BasePixiPythonOperator`][airflow.providers.pixi.operators.pixi.BasePixiPythonOperator], as
    ``class PixiDecoratedOperator(BasePixiDecoratedOperator, PixiOperator)``, and set ``custom_operator_name``. The
    subclass gets the operator's ``template_fields``, ``template_fields_renderers`` and ``get_python_source`` in place
    of ``DecoratedOperator``'s, unless a class in front of ``DecoratedOperator`` sets them.
    """

    _FROM_THE_OPERATOR = ("template_fields", "template_fields_renderers", "get_python_source")

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        operator = next(
            (c for c in cls.__mro__ if issubclass(c, BasePixiPythonOperator) and not issubclass(c, DecoratedOperator)),
            None,
        )
        if operator is None:
            return
        for name in cls._FROM_THE_OPERATOR:
            owner = next(c for c in cls.__mro__ if name in c.__dict__)
            if owner in DecoratedOperator.__mro__:
                setattr(cls, name, operator.__dict__.get(name, getattr(operator, name)))

    def __init__(
        self,
        *,
        python_callable: Callable[..., Any],
        op_args: Sequence[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        # DecoratedOperator sets op_args/op_kwargs itself, then calls the upstream
        # __init__ with kwargs_to_upstream; the Pixi operator must see the same values
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


class PixiDecoratedOperator(BasePixiDecoratedOperator, PixiOperator):  # type: ignore[misc]
    """``@task.pixi``: run the function inside a Pixi environment."""

    custom_operator_name = "@task.pixi"


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
