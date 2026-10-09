"""Environment variables of a pixi run from Airflow Variables and Connections, read on the worker when it runs.

Values never pass through templates. Passwords, connection URIs and extras are masked in the task log.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

from airflow.sdk import Connection, Variable
from airflow.sdk.log import mask_secret

from airflow.providers.pixi.utils.compat import AirflowException, AirflowNotFoundException

CONNECTION_FIELDS = ("host", "login", "password", "schema", "port", "extra")
"""The fields ``env_from_connections`` can name after the connection id, as ``"<conn_id>.<field>"``."""

_REFERENCE = re.compile(rf"^(?P<conn_id>.+?)(?:\.(?P<field>{'|'.join(CONNECTION_FIELDS)})(?:\.(?P<key>.+))?)?$")


def parse_connection_reference(reference: str) -> tuple[str, str | None, str | None]:
    """Split an ``env_from_connections`` value into the connection id, the field and the key of ``extra``.

    ``"conn_id"`` is the connection's URI (field ``None``), ``"conn_id.password"`` one field, and
    ``"conn_id.extra.key"`` one key of the extra. The first ``.<field>`` names the field, so a connection id
    cannot itself contain ``.host``, ``.login``, ``.password``, ``.schema``, ``.port`` or ``.extra``. Only
    ``extra`` takes a key: ``"conn_id.port.internal"`` is rejected rather than read as the whole port.
    """
    match = _REFERENCE.match(reference) if isinstance(reference, str) else None
    if match is None:
        raise ValueError(f"{reference!r} is not a connection id, '<conn_id>.<field>' or '<conn_id>.extra.<key>'")
    if match["key"] is not None and match["field"] != "extra":
        raise ValueError(
            f"{reference!r}: only extra has keys, as in '<conn_id>.extra.<key>'; {match['field']} is a single value, "
            f"'{match['conn_id']}.{match['field']}'"
        )
    return match["conn_id"], match["field"], match["key"]


def check_env_sources(
    env_from_variables: Mapping[str, str] | None, env_from_connections: Mapping[str, str] | None
) -> None:
    """Raise ``ValueError`` or ``TypeError`` for ``env_from_variables`` / ``env_from_connections`` that cannot be resolved."""
    for argument, mapping in (
        ("env_from_variables", env_from_variables),
        ("env_from_connections", env_from_connections),
    ):
        if mapping is None:
            continue
        if not isinstance(mapping, Mapping):
            raise TypeError(f"{argument} must be a dict of environment variable names to ids, not {mapping!r}")
        for name, source in mapping.items():
            if not (isinstance(name, str) and name) or "=" in name:
                raise ValueError(f"{argument}: {name!r} is not an environment variable name")
            if not (isinstance(source, str) and source):
                raise ValueError(f"{argument}[{name!r}] must be a non-empty string, not {source!r}")
    for reference in (env_from_connections or {}).values():
        parse_connection_reference(reference)
    if both := set(env_from_variables or {}) & set(env_from_connections or {}):
        raise ValueError(f"{', '.join(sorted(both))} set by both env_from_variables and env_from_connections")


def _connection_value(conn: Any, field: str | None, key: str | None) -> tuple[str | None, bool]:
    """Return the value of a connection field, or of its URI, and whether it is a secret."""
    if field is None:
        return conn.get_uri(), True
    if field == "extra" and key is not None:
        value = conn.extra_dejson.get(key)
        return (value if isinstance(value, str) or value is None else json.dumps(value)), True
    value = getattr(conn, field)
    return (None if value in (None, "") else str(value)), field in ("password", "extra")


def resolve_env(
    *,
    env_from_variables: Mapping[str, str] | None = None,
    env_from_connections: Mapping[str, str] | None = None,
    optional_variables: Mapping[str, str | None] | None = None,
) -> dict[str, str]:
    """Return the environment variables read from Airflow Variables and Connections, for the worker's run.

    :param env_from_variables: environment variable name to the key of a Variable, which must exist.
    :param env_from_connections: environment variable name to ``"conn_id"`` (the connection's URI),
        ``"conn_id.<field>"`` (``host``, ``login``, ``password``, ``schema``, ``port``, or ``extra`` as a JSON
        string) or ``"conn_id.extra.<key>"`` (one key of the extra; a value that is not a string as JSON). The
        connection and the field must exist.
    :param optional_variables: environment variable name to the key of a Variable whose value, stripped of
        surrounding whitespace, is used if the Variable exists; ``None`` or a missing Variable leaves it out. For
        the cache directory Variables.

    ``env_from_variables`` and ``env_from_connections`` win over ``optional_variables``. Passwords, URIs and
    extras of connections are masked in the task log; Variables are masked as Airflow masks them, by their key.
    """
    check_env_sources(env_from_variables, env_from_connections)
    env: dict[str, str] = {}
    for name, key in (optional_variables or {}).items():
        if key:
            value = Variable.get(key, default=None)
            if value is not None:
                env[name] = str(value).strip()
    for name, key in (env_from_variables or {}).items():
        value = Variable.get(key, default=None)
        if value is None:
            raise AirflowException(f"Variable {key!r} for {name} in env_from_variables does not exist")
        env[name] = str(value)
    connections: dict[str, Any] = {}
    for name, reference in (env_from_connections or {}).items():
        conn_id, field, key = parse_connection_reference(reference)
        if conn_id not in connections:
            try:
                connections[conn_id] = Connection.get(conn_id)
            except AirflowNotFoundException:
                raise AirflowException(
                    f"Connection {conn_id!r} for {name} in env_from_connections does not exist"
                ) from None
        value, secret = _connection_value(connections[conn_id], field, key)
        if value is None:
            what = f"extra key {key!r}" if key is not None else field
            raise AirflowException(f"Connection {conn_id!r} has no {what}, which env_from_connections sets as {name}")
        if secret:
            mask_secret(value)
        env[name] = value
    return env
