"""Apache Airflow provider for Pixi: run Python callables inside Pixi environments."""

from __future__ import annotations

from typing import Any

__version__ = "0.1.0"

# Lazy: Airflow imports this package for get_provider_info while its own configuration is still
# initialising, so importing operators here is circular. PixiKubernetesPodOperator needs the
# cncf.kubernetes extra, so it is imported from airflow.providers.pixi.operators.kubernetes only.
_EXPORTS = {
    "PixiBashOperator": "airflow.providers.pixi.operators.bash",
    "PixiOperator": "airflow.providers.pixi.operators.pixi",
    "PixiSensor": "airflow.providers.pixi.sensors.pixi",
}

__all__ = ["PixiBashOperator", "PixiOperator", "PixiSensor", "__version__"]


def __getattr__(name: str) -> Any:
    if name in _EXPORTS:
        import importlib

        return getattr(importlib.import_module(_EXPORTS[name]), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
