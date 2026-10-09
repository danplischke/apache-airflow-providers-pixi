"""The part of the Airflow context a callable in a Pixi environment receives, as JSON values.

The environment may not have Airflow or pendulum, so the context travels as plain JSON: dates and datetimes as
ISO 8601 strings, ``params`` and the DAG run's ``conf`` as dicts.
"""

from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any

_PLAIN_KEYS = (
    "run_id",
    "ds",
    "ds_nodash",
    "ts",
    "ts_nodash",
    "ts_nodash_with_tz",
    "task_instance_key_str",
    "test_mode",
    "try_number",
    "expanded_ti_count",
    "task_reschedule_count",
    "partition_key",
    "logical_date",
    "data_interval_start",
    "data_interval_end",
    "prev_data_interval_start_success",
    "prev_data_interval_end_success",
    "prev_start_date_success",
    "prev_end_date_success",
    "partition_date",
)
_TASK_INSTANCE_KEYS = ("dag_id", "task_id", "map_index", "try_number")

CONTEXT_KEYS: tuple[str, ...] = (*dict.fromkeys((*_PLAIN_KEYS, *_TASK_INSTANCE_KEYS, "params", "conf")),)
"""The keys a callable can receive, when the task's context has them."""


class _NotJson(Exception):
    pass


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return _json_items(value)
    if isinstance(value, (list, tuple)):
        return [_json_value(v) for v in value]
    raise _NotJson


def _json_items(mapping: Mapping[Any, Any]) -> dict[str, Any]:
    result = {}
    for key, value in mapping.items():
        if not isinstance(key, str):
            continue
        try:
            result[key] = _json_value(value)
        except _NotJson:
            pass
    return result


def serializable_context(context: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return the context values of [`CONTEXT_KEYS`][CONTEXT_KEYS] that ``context`` has, as JSON values.

    Dates and datetimes become ISO 8601 strings, ``params`` a plain dict and the DAG run's ``conf`` ``conf``.
    ``dag_id``, ``task_id``, ``map_index`` and ``try_number`` come from the task instance. A value JSON cannot
    hold is left out, as is a key the context does not have.
    """
    if not context:
        return {}
    result: dict[str, Any] = {}

    def add(key: str, value: Any) -> None:
        try:
            result[key] = _json_value(value)
        except _NotJson:
            pass

    for key in _PLAIN_KEYS:
        if key in context:
            add(key, context[key])
    ti = context.get("ti") or context.get("task_instance")
    for key in _TASK_INSTANCE_KEYS:
        if key not in result and ti is not None and hasattr(ti, key):
            add(key, getattr(ti, key))
    if "dag_id" not in result and hasattr(context.get("dag"), "dag_id"):
        add("dag_id", context["dag"].dag_id)
    if "task_id" not in result and hasattr(context.get("task"), "task_id"):
        add("task_id", context["task"].task_id)
    params = context.get("params")
    if isinstance(params, Mapping):
        result["params"] = _json_items(params.dump() if callable(getattr(type(params), "dump", None)) else params)
    conf = getattr(context.get("dag_run"), "conf", None)
    if isinstance(conf, Mapping):
        result["conf"] = _json_items(conf)
    return result
