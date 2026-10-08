"""Apache Airflow provider for Pixi: run Python callables inside Pixi environments."""

from __future__ import annotations

from typing import Any

__version__ = "0.1.0"

__all__ = [
    "PixiOperator",
    "__version__",
]


def __getattr__(name: str) -> Any:
    # Lazy: Airflow imports this package for get_provider_info while its own
    # configuration is still initialising, so importing operators here is circular.
    if name in __all__:
        from airflow.providers.pixi import operators

        return getattr(operators, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
