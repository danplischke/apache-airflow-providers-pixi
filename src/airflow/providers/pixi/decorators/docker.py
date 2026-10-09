"""``@task.pixi_docker``: run the function inside a Pixi environment in a Docker container."""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any


@functools.cache
def _decorated_operator_class() -> type:
    from airflow.providers.pixi.decorators.pixi import BasePixiDecoratedOperator
    from airflow.providers.pixi.operators.docker import PixiDockerOperator

    class PixiDockerDecoratedOperator(BasePixiDecoratedOperator, PixiDockerOperator):  # type: ignore[misc]
        """``@task.pixi_docker``: run the function inside a Pixi environment in a Docker container."""

        custom_operator_name = "@task.pixi_docker"

    return PixiDockerDecoratedOperator


def pixi_docker_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    **kwargs: Any,
):
    """``@task.pixi_docker``: run the function inside a Pixi environment in a Docker container.

    Accepts every ``PixiDockerOperator`` argument. As for ``@task.pixi``, the function's source is shipped, so it
    must be self-contained. Requires ``pip install "apache-airflow-providers-pixi[docker]"``.
    """
    from airflow.sdk.bases.decorator import task_decorator_factory

    try:
        operator_class = _decorated_operator_class()
    except ImportError as e:
        raise ImportError(
            "@task.pixi_docker requires apache-airflow-providers-docker. "
            'Install with: pip install "apache-airflow-providers-pixi[docker]"'
        ) from e
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=operator_class,
        **kwargs,
    )
