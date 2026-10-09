"""``@task.pixi_sensor``: a sensor whose function runs inside a Pixi environment on each poke."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from airflow.providers.pixi.sensors.pixi import PixiSensor
from airflow.sdk.bases.decorator import get_unique_task_id, task_decorator_factory


class PixiDecoratedSensorOperator(PixiSensor):
    """``@task.pixi_sensor``: a sensor whose function runs inside a Pixi environment on each poke."""

    custom_operator_name = "@task.pixi_sensor"
    shallow_copy_attrs: Sequence[str] = ("python_callable",)

    def __init__(self, *, task_id: str, **kwargs: Any) -> None:
        kwargs["task_id"] = get_unique_task_id(task_id, kwargs.get("dag"), kwargs.get("task_group"))
        super().__init__(**kwargs)


def pixi_sensor_task(python_callable: Callable[..., Any] | None = None, **kwargs: Any):
    """``@task.pixi_sensor``: a sensor whose function runs inside a Pixi environment on each poke.

    Like ``@task.sensor``. The function returns a truthy value when the condition is met, or
    ``{"is_done": ..., "xcom_value": ...}`` to also push an XCom. Accepts every ``PixiSensor`` argument.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=False,
        decorated_operator_class=PixiDecoratedSensorOperator,
        **kwargs,
    )
