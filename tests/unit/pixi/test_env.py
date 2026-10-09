"""Environment variables of a run from Airflow Variables and Connections; a fake pixi runs the callable.

Variables and connections come from ``AIRFLOW_VAR_<KEY>`` and ``AIRFLOW_CONN_<CONN_ID>``, which the Task SDK
reads as it reads a secrets backend.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from airflow.sdk import DAG

from airflow.providers.pixi.operators.pixi import PixiOperator
from airflow.providers.pixi.utils.compat import AirflowException
from airflow.providers.pixi.utils.env import parse_connection_reference, resolve_env

INLINE = {"dependencies": {"python": "3.12.*"}}
WAREHOUSE = {
    "conn_type": "postgres",
    "host": "db.example.com",
    "login": "etl",
    "password": "s3cret-password",
    "schema": "sales",
    "port": 5432,
    "extra": {"sslmode": "require", "options": {"timeout": 30}},
}


@pytest.fixture
def warehouse(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_CONN_WAREHOUSE", json.dumps(WAREHOUSE))


def environment(names):
    import os

    return {name: os.environ.get(name) for name in names}


def make(fake_pixi, **kwargs) -> PixiOperator:
    return PixiOperator(task_id="t", python_callable=environment, pixi_binary=str(fake_pixi.path), **INLINE, **kwargs)


def run(op: PixiOperator, *names: str) -> dict:
    op.op_args = [list(names)]
    return op.execute({"ti": MagicMock()})


@pytest.mark.parametrize(
    ("reference", "expected"),
    [
        ("warehouse", ("warehouse", None, None)),
        ("warehouse.password", ("warehouse", "password", None)),
        ("warehouse.extra", ("warehouse", "extra", None)),
        ("warehouse.extra.sslmode", ("warehouse", "extra", "sslmode")),
        ("warehouse.extra.a.b", ("warehouse", "extra", "a.b")),
        ("my.db.host", ("my.db", "host", None)),
        ("my.db", ("my.db", None, None)),
    ],
)
def test_connection_references(reference: str, expected) -> None:
    assert parse_connection_reference(reference) == expected


def test_variables_and_connection_fields_reach_the_run(fake_pixi, warehouse, monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_API_URL", "https://api.example.com")
    op = make(
        fake_pixi,
        env_from_variables={"API_URL": "api_url"},
        env_from_connections={
            "DB_HOST": "warehouse.host",
            "DB_USER": "warehouse.login",
            "DB_PASSWORD": "warehouse.password",
            "DB_NAME": "warehouse.schema",
            "DB_PORT": "warehouse.port",
            "DB_EXTRA": "warehouse.extra",
            "DB_SSLMODE": "warehouse.extra.sslmode",
            "DB_OPTIONS": "warehouse.extra.options",
            "DB_URI": "warehouse",
        },
    )
    env = run(op, "API_URL", "DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_PORT", "DB_EXTRA", "DB_SSLMODE")
    assert env == {
        "API_URL": "https://api.example.com",
        "DB_HOST": "db.example.com",
        "DB_USER": "etl",
        "DB_PASSWORD": "s3cret-password",
        "DB_NAME": "sales",
        "DB_PORT": "5432",
        "DB_EXTRA": json.dumps(WAREHOUSE["extra"]),
        "DB_SSLMODE": "require",
    }
    env = run(op, "DB_OPTIONS", "DB_URI")
    assert json.loads(env["DB_OPTIONS"]) == {"timeout": 30}
    assert env["DB_URI"].startswith("postgres://etl:s3cret-password@db.example.com:5432/sales")


def test_secrets_are_masked(warehouse) -> None:
    with patch("airflow.providers.pixi.utils.env.mask_secret") as mask:
        env = resolve_env(
            env_from_connections={
                "HOST": "warehouse.host",
                "PASSWORD": "warehouse.password",
                "URI": "warehouse",
                "EXTRA": "warehouse.extra",
                "SSLMODE": "warehouse.extra.sslmode",
            }
        )
    masked = {call.args[0] for call in mask.call_args_list}
    assert masked == {env["PASSWORD"], env["URI"], env["EXTRA"], env["SSLMODE"]}
    assert "db.example.com" not in masked


def test_precedence(fake_pixi, warehouse, monkeypatch) -> None:
    monkeypatch.setenv("PIXI_CACHE_DIR", "/worker")
    monkeypatch.setenv("UV_CACHE_DIR", "/worker")
    monkeypatch.setenv("PIP_CACHE_DIR", "/worker")
    monkeypatch.setenv("DB_HOST", "/worker")
    monkeypatch.setenv("AIRFLOW_VAR_PIXI_CACHE", "/variable")
    monkeypatch.setenv("AIRFLOW_VAR_UV_CACHE", "/variable")
    monkeypatch.setenv("AIRFLOW_VAR_OVERRIDE", "/env-from-variables")
    op = make(
        fake_pixi,
        pixi_cache_dir_variable="pixi_cache",
        uv_cache_dir_variable="uv_cache",
        env_from_variables={"UV_CACHE_DIR": "override"},
        env_from_connections={"DB_HOST": "warehouse.host"},
        env_vars={"PIP_CACHE_DIR": "/env-vars", "DB_HOST": "/env-vars"},
    )
    assert run(op, "PIXI_CACHE_DIR", "UV_CACHE_DIR", "PIP_CACHE_DIR", "DB_HOST") == {
        "PIXI_CACHE_DIR": "/variable",
        "UV_CACHE_DIR": "/env-from-variables",
        "PIP_CACHE_DIR": "/env-vars",
        "DB_HOST": "/env-vars",
    }


def test_a_missing_cache_variable_leaves_the_variable_unset(fake_pixi, monkeypatch) -> None:
    monkeypatch.delenv("PIXI_CACHE_DIR", raising=False)
    assert run(make(fake_pixi, pixi_cache_dir_variable="no_such_variable"), "PIXI_CACHE_DIR") == {
        "PIXI_CACHE_DIR": None
    }


def test_a_missing_variable_fails_before_pixi_runs(fake_pixi) -> None:
    op = make(fake_pixi, env_from_variables={"API_URL": "no_such_variable"})
    with pytest.raises(AirflowException, match="Variable 'no_such_variable' for API_URL in env_from_variables"):
        run(op, "API_URL")
    assert fake_pixi.calls == []


def test_a_missing_connection_fails_before_pixi_runs(fake_pixi) -> None:
    op = make(fake_pixi, env_from_connections={"DB": "no_such_connection.password"})
    with pytest.raises(AirflowException, match="Connection 'no_such_connection' for DB in env_from_connections"):
        run(op, "DB")
    assert fake_pixi.calls == []


@pytest.mark.parametrize(
    ("reference", "error"),
    [("warehouse.extra.no_such_key", "has no extra key 'no_such_key'"), ("nologin.login", "has no login")],
)
def test_a_missing_field_fails(warehouse, monkeypatch, reference: str, error: str) -> None:
    monkeypatch.setenv("AIRFLOW_CONN_NOLOGIN", json.dumps({"conn_type": "http", "host": "example.com"}))
    with pytest.raises(AirflowException, match=f"{error}, which env_from_connections sets as X"):
        resolve_env(env_from_connections={"X": reference})


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"env_from_variables": ["API_URL"]}, "must be a dict"),
        ({"env_from_variables": {"": "key"}}, "is not an environment variable name"),
        ({"env_from_variables": {"A=B": "key"}}, "is not an environment variable name"),
        ({"env_from_connections": {"A": ""}}, "must be a non-empty string"),
        ({"env_from_variables": {"A": "a"}, "env_from_connections": {"A": "c"}}, "A set by both"),
        ({"env_from_connections": {"DB_PORT": "warehouse.port.internal"}}, "only extra has keys"),
        ({"env_from_connections": {"TOKEN": "api.password.v2"}}, "'api.password'"),
    ],
)
def test_invalid_sources_are_rejected_when_the_dag_is_parsed(kwargs, error: str) -> None:
    with pytest.raises((ValueError, TypeError), match=error):
        PixiOperator(task_id="t", python_callable=environment, **INLINE, **kwargs)


def test_ids_and_values_are_not_templated(fake_pixi, monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_TEMPLATED", "{{ ds }}")
    op = make(fake_pixi, env_from_variables={"VALUE": "templated"})
    assert "env_from_variables" not in op.template_fields
    assert "env_from_connections" not in op.template_fields
    assert run(op, "VALUE") == {"VALUE": "{{ ds }}"}


def test_default_args_set_them_for_every_task(fake_pixi, monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_API_URL", "https://api.example.com")
    with DAG("pipeline", default_args={"env_from_variables": {"API_URL": "api_url"}}):
        op = make(fake_pixi)
    assert run(op, "API_URL") == {"API_URL": "https://api.example.com"}
