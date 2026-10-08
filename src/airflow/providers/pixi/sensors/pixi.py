"""Wait for a Python callable running inside a Pixi environment to report success."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from airflow.sdk.bases.sensor import BaseSensorOperator, PokeReturnValue

from airflow.providers.pixi.operators.pixi import BasePixiPythonOperator, PixiSubprocessMixin

_POKE_RESULT_KEYS = {"is_done", "xcom_value"}


class PixiSensor(PixiSubprocessMixin, BasePixiPythonOperator, BaseSensorOperator):
    """Wait for a Python callable, run inside a Pixi environment on each poke, to return a truthy value.

    Like ``PythonSensor``, but the callable runs in the Pixi environment, as for
    :class:`~airflow.providers.pixi.operators.pixi.PixiOperator`, whose arguments it accepts, together with
    every ``BaseSensorOperator`` argument (``poke_interval``, ``timeout``, ``mode``, ``soft_fail``, ...).

    To finish with an XCom value, return ``{"is_done": True, "xcom_value": value}``, the fields of
    ``PokeReturnValue``, which Airflow cannot pass into the environment itself.

    In ``mode="poke"`` the environment is prepared once for all pokes of a run. In ``mode="reschedule"``
    each poke is a new run, so pass ``env_cache_path`` to keep an inline environment between them.
    """

    template_fields: Sequence[str] = (*BasePixiPythonOperator.template_fields, *PixiSubprocessMixin.template_fields)
    custom_operator_name = "PixiSensor"

    def execute(self, context: Any) -> Any:
        with self.holding_manifest():
            return super().execute(context)

    def poke(self, context: Any) -> PokeReturnValue:
        result = self.run_callable()
        if isinstance(result, dict) and "is_done" in result and set(result) <= _POKE_RESULT_KEYS:
            return PokeReturnValue(bool(result["is_done"]), result.get("xcom_value"))
        return PokeReturnValue(bool(result))
