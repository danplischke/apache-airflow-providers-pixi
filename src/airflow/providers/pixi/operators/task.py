"""Deprecated: [`PixiTaskOperator`][PixiTaskOperator] is now
[`PixiProjectTaskOperator`][airflow.providers.pixi.operators.project_task.PixiProjectTaskOperator], in
``airflow.providers.pixi.operators.project_task``."""

from __future__ import annotations

import warnings
from typing import Any

from airflow.providers.pixi.operators.project_task import PixiProjectTaskOperator
from airflow.providers.pixi.utils.compat import AirflowProviderDeprecationWarning

__all__ = ("PixiTaskOperator",)


class PixiTaskOperator(PixiProjectTaskOperator):
    """Deprecated name of [`PixiProjectTaskOperator`][airflow.providers.pixi.operators.project_task.PixiProjectTaskOperator]."""

    custom_operator_name = "PixiTask"

    def __init__(self, **kwargs: Any) -> None:
        warnings.warn(
            "PixiTaskOperator is deprecated; use PixiProjectTaskOperator from "
            "airflow.providers.pixi.operators.project_task instead",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        super().__init__(**kwargs)
