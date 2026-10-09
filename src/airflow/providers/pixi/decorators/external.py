"""``@task.pixi_external``: run the function with the Python of a Pixi environment that is already installed."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from airflow.providers.pixi.decorators.pixi import BasePixiDecoratedOperator
from airflow.providers.pixi.operators.external import PixiExternalPythonOperator
from airflow.sdk.bases.decorator import task_decorator_factory


class PixiExternalDecoratedOperator(BasePixiDecoratedOperator, PixiExternalPythonOperator):  # type: ignore[misc]
    """``@task.pixi_external``: run the function with the Python of a Pixi environment that is already installed."""

    custom_operator_name = "@task.pixi_external"


def pixi_external_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    **kwargs: Any,
):
    """``@task.pixi_external``: run the function with the Python of a Pixi environment that is already installed.

    Like ``@task.external_python``, for environments installed with ``pixi install`` when the image was built: no
    pixi is needed at run time. Accepts every ``PixiExternalPythonOperator`` argument; as for ``@task.pixi``, the
    function's source is shipped, so it must be self-contained.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=PixiExternalDecoratedOperator,
        **kwargs,
    )
