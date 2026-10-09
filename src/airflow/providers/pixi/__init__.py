"""Apache Airflow provider for Pixi: run Python callables inside Pixi environments."""

from __future__ import annotations

from typing import Any

__version__ = "0.1.0"

_EXPORTS = {
    "PixiBashOperator": "airflow.providers.pixi.operators.bash",
    "PixiBranchOperator": "airflow.providers.pixi.operators.pixi",
    "PixiCallableError": "airflow.providers.pixi.exceptions",
    "PixiHook": "airflow.providers.pixi.hooks.pixi",
    "PixiOperator": "airflow.providers.pixi.operators.pixi",
    "PixiSensor": "airflow.providers.pixi.sensors.pixi",
    "PixiShortCircuitOperator": "airflow.providers.pixi.operators.pixi",
    "PixiTaskOperator": "airflow.providers.pixi.operators.task",
}

__all__ = [
    "PixiBashOperator",
    "PixiBranchOperator",
    "PixiCallableError",
    "PixiHook",
    "PixiOperator",
    "PixiSensor",
    "PixiShortCircuitOperator",
    "PixiTaskOperator",
    "__version__",
]


def __getattr__(name: str) -> Any:
    if name in _EXPORTS:
        import importlib

        return getattr(importlib.import_module(_EXPORTS[name]), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
