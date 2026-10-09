from __future__ import annotations

from typing import Any

_EXPORTS = {
    "PixiBashOperator": "airflow.providers.pixi.operators.bash",
    "PixiBranchOperator": "airflow.providers.pixi.operators.pixi",
    "PixiOperator": "airflow.providers.pixi.operators.pixi",
    "PixiShortCircuitOperator": "airflow.providers.pixi.operators.pixi",
    "PixiTaskOperator": "airflow.providers.pixi.operators.task",
}

__all__ = [
    "PixiBashOperator",
    "PixiBranchOperator",
    "PixiOperator",
    "PixiShortCircuitOperator",
    "PixiTaskOperator",
]


def __getattr__(name: str) -> Any:
    if name in _EXPORTS:
        import importlib

        return getattr(importlib.import_module(_EXPORTS[name]), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
