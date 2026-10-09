"""``@task.pixi_kubernetes``: run the function inside a Pixi environment in a Kubernetes pod."""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any


@functools.cache
def _decorated_operator_class() -> type:
    from airflow.providers.pixi.decorators.pixi import BasePixiDecoratedOperator
    from airflow.providers.pixi.operators.kubernetes import PixiKubernetesPodOperator

    class PixiKubernetesDecoratedOperator(BasePixiDecoratedOperator, PixiKubernetesPodOperator):  # type: ignore[misc]
        """``@task.pixi_kubernetes``: run the function inside a Pixi environment in a Kubernetes pod."""

        custom_operator_name = "@task.pixi_kubernetes"

    return PixiKubernetesDecoratedOperator


def pixi_kubernetes_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    **kwargs: Any,
):
    """``@task.pixi_kubernetes``: run the function inside a Pixi environment in a Kubernetes pod.

    Accepts every ``PixiKubernetesPodOperator`` argument. As for ``@task.pixi``, the function's source is
    shipped, so it must be self-contained. Requires
    ``pip install "apache-airflow-providers-pixi[cncf.kubernetes]"``.
    """
    from airflow.sdk.bases.decorator import task_decorator_factory

    try:
        operator_class = _decorated_operator_class()
    except ImportError as e:
        raise ImportError(
            "@task.pixi_kubernetes requires apache-airflow-providers-cncf-kubernetes. "
            'Install with: pip install "apache-airflow-providers-pixi[cncf.kubernetes]"'
        ) from e
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=operator_class,
        **kwargs,
    )
