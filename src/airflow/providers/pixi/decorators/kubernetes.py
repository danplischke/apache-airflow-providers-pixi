"""``@task.pixi_kubernetes``: run the function inside a Pixi environment in a Kubernetes pod."""

from __future__ import annotations

import functools
from collections.abc import Callable, Mapping, Sequence
from typing import Any


@functools.cache
def _decorated_operator_class() -> type:
    from airflow.sdk.bases.decorator import DecoratedOperator

    from airflow.providers.pixi.operators.kubernetes import PixiKubernetesPodOperator

    class PixiKubernetesDecoratedOperator(DecoratedOperator, PixiKubernetesPodOperator):  # type: ignore[misc]
        """``@task.pixi_kubernetes``: run the function inside a Pixi environment in a Kubernetes pod."""

        custom_operator_name = "@task.pixi_kubernetes"
        template_fields = PixiKubernetesPodOperator.template_fields
        template_fields_renderers = PixiKubernetesPodOperator.template_fields_renderers

        def __init__(
            self,
            *,
            python_callable: Callable[..., Any],
            op_args: Sequence[Any] | None = None,
            op_kwargs: Mapping[str, Any] | None = None,
            **kwargs: Any,
        ) -> None:
            # as for @task.pixi: PixiKubernetesPodOperator has to see the values DecoratedOperator keeps
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
