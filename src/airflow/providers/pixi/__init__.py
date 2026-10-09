"""Apache Airflow provider for Pixi: run Python callables inside Pixi environments."""

from __future__ import annotations

from typing import Any

__version__: str

_EXPORTS = {
    "PixiBashOperator": "airflow.providers.pixi.operators.bash",
    "PixiBranchOperator": "airflow.providers.pixi.operators.pixi",
    "PixiCallableError": "airflow.providers.pixi.exceptions",
    "PixiExternalPythonOperator": "airflow.providers.pixi.operators.external",
    "PixiHook": "airflow.providers.pixi.hooks.pixi",
    "PixiOperator": "airflow.providers.pixi.operators.pixi",
    "PixiProjectTaskOperator": "airflow.providers.pixi.operators.project_task",
    "PixiSensor": "airflow.providers.pixi.sensors.pixi",
    "PixiShortCircuitOperator": "airflow.providers.pixi.operators.pixi",
    "PixiTaskOperator": "airflow.providers.pixi.operators.task",  # deprecated, warns when instantiated
}

__all__ = (
    "PixiBashOperator",
    "PixiBranchOperator",
    "PixiCallableError",
    "PixiExternalPythonOperator",
    "PixiHook",
    "PixiOperator",
    "PixiProjectTaskOperator",
    "PixiSensor",
    "PixiShortCircuitOperator",
    "PixiTaskOperator",
    "__version__",
)


def __getattr__(name: str) -> Any:
    if name == "__version__":
        from importlib.metadata import PackageNotFoundError, version

        try:
            return version("apache-airflow-providers-pixi")
        except PackageNotFoundError:
            return "0.0.0"
    if name in _EXPORTS:
        import importlib

        return getattr(importlib.import_module(_EXPORTS[name]), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
