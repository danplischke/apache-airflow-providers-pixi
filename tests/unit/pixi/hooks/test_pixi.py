"""Credentials of ``pixi`` connections in pixi's credentials file and in netrc.

Connections come from ``AIRFLOW_CONN_<CONN_ID>``, which the Task SDK reads as it reads a secrets backend.
"""

from __future__ import annotations

import contextlib
import json
import os
import stat
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.pixi.hooks.pixi import UI_FIELD_BEHAVIOUR, PixiHook, pixi_auth_env
from airflow.providers.pixi.operators.pixi import PixiOperator
from airflow.providers.pixi.utils.compat import AirflowException, AirflowNotFoundException

INLINE = {"dependencies": {"python": "3.12.*"}}


@pytest.fixture
def connection(monkeypatch):
    """Define the ``pixi`` connection ``conn_id``."""

    def define(conn_id: str, **fields) -> None:
        extra = fields.pop("extra", None)
        value = {"conn_type": "pixi", **fields, **({"extra": extra} if extra is not None else {})}
        monkeypatch.setenv(f"AIRFLOW_CONN_{conn_id.upper()}", json.dumps(value))

    return define


@pytest.fixture
def home(tmp_path: Path) -> Path:
    """A home directory without credentials, so the worker's own are never read."""
    directory = tmp_path / "home"
    directory.mkdir()
    return directory


def auth_files(conn_ids, env) -> tuple[dict[str, str], dict[str, str]]:
    """Return the variables pixi_auth_env sets and the contents of the files, read while they exist."""
    with pixi_auth_env(conn_ids, env) as auth:
        contents = {name: Path(path).read_text() for name, path in auth.items()}
        for path in auth.values():
            assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    for path in auth.values():
        assert not os.path.exists(path)
        assert not os.path.exists(os.path.dirname(path))
    return auth, contents


@pytest.mark.parametrize(
    ("fields", "entry"),
    [
        ({"password": "pfx_token"}, {"BearerToken": "pfx_token"}),
        ({"password": "pfx_token", "extra": {"auth_type": "bearer_token"}}, {"BearerToken": "pfx_token"}),
        ({"password": "conda_tok", "extra": {"auth_type": "conda_token"}}, {"CondaToken": "conda_tok"}),
        ({"login": "me", "password": "pw"}, {"BasicHTTP": {"username": "me", "password": "pw"}}),
        (
            {"login": "me", "password": "pw", "extra": {"extra__pixi__auth_type": "basic_http"}},
            {"BasicHTTP": {"username": "me", "password": "pw"}},
        ),
    ],
)
def test_credentials_file_entries(connection, home: Path, fields, entry) -> None:
    connection("channel", host="repo.prefix.dev", **fields)
    auth, contents = auth_files("channel", {"HOME": str(home)})
    assert set(auth) == {"RATTLER_AUTH_FILE"}
    assert json.loads(contents["RATTLER_AUTH_FILE"]) == {"repo.prefix.dev": entry}


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        ("*.prefix.dev", "*.prefix.dev"),
        ("https://Artifactory.example.com:8443/api/conda/channel", "artifactory.example.com"),
        ("repo.prefix.dev/channel", "repo.prefix.dev"),
    ],
)
def test_host_is_reduced_to_the_host_name(connection, home: Path, host: str, expected: str) -> None:
    connection("channel", host=host, password="token")
    _, contents = auth_files("channel", {"HOME": str(home)})
    assert list(json.loads(contents["RATTLER_AUTH_FILE"])) == [expected]


def test_several_connections_and_the_existing_credentials_file(connection, home: Path, tmp_path: Path) -> None:
    connection("prefix", host="*.prefix.dev", password="new_token")
    connection("artifactory", host="artifactory.example.com", login="me", password="pw")
    existing = tmp_path / "credentials.json"
    existing.write_text(json.dumps({"*.prefix.dev": {"BearerToken": "old"}, "other.org": {"CondaToken": "keep"}}))
    _, contents = auth_files(["prefix", "artifactory"], {"HOME": str(home), "RATTLER_AUTH_FILE": str(existing)})
    assert json.loads(contents["RATTLER_AUTH_FILE"]) == {
        "*.prefix.dev": {"BearerToken": "new_token"},
        "other.org": {"CondaToken": "keep"},
        "artifactory.example.com": {"BasicHTTP": {"username": "me", "password": "pw"}},
    }
    assert json.loads(existing.read_text())["*.prefix.dev"] == {"BearerToken": "old"}


def test_the_default_credentials_file_is_kept(connection, home: Path) -> None:
    (home / ".rattler").mkdir()
    (home / ".rattler" / "credentials.json").write_text(json.dumps({"other.org": {"BearerToken": "keep"}}))
    connection("channel", host="repo.prefix.dev", password="token")
    _, contents = auth_files("channel", {"HOME": str(home)})
    assert set(json.loads(contents["RATTLER_AUTH_FILE"])) == {"other.org", "repo.prefix.dev"}


def test_netrc_for_a_pypi_index(connection, home: Path, tmp_path: Path) -> None:
    connection("index", host="pypi.example.com", login="me", password="pw", extra={"auth_type": "netrc"})
    (home / ".netrc").write_text("machine other.org login a password b")
    auth, contents = auth_files("index", {"HOME": str(home)})
    assert set(auth) == {"NETRC"}
    assert (
        contents["NETRC"] == "machine pypi.example.com\nlogin me\npassword pw\nmachine other.org login a password b\n"
    )
    custom = tmp_path / "custom-netrc"
    custom.write_text("machine custom.org login c password d\n")
    _, contents = auth_files("index", {"HOME": str(home), "NETRC": str(custom)})
    assert contents["NETRC"].endswith("machine custom.org login c password d\n")


def test_channel_and_index_credentials_together(connection, home: Path) -> None:
    connection("channel", host="repo.prefix.dev", password="token")
    connection("index", host="repo.prefix.dev", login="me", password="pw", extra={"auth_type": "netrc"})
    auth, _ = auth_files(["channel", "index"], {"HOME": str(home)})
    assert set(auth) == {"RATTLER_AUTH_FILE", "NETRC"}


def test_no_connections_set_nothing() -> None:
    for conn_ids in (None, []):
        with pixi_auth_env(conn_ids, {}) as auth:
            assert auth == {}


def test_the_secret_is_masked(connection) -> None:
    connection("channel", host="repo.prefix.dev", password="pfx_secret_token")
    with patch("airflow.providers.pixi.hooks.pixi.mask_secret") as mask:
        credentials = PixiHook("channel").get_credentials()
    mask.assert_called_once_with("pfx_secret_token")
    assert "pfx_secret_token" not in repr(credentials)


@pytest.mark.parametrize(
    ("fields", "error"),
    [
        ({"password": "token"}, "has no host"),
        ({"host": "repo.prefix.dev"}, "has no password"),
        ({"host": "h.org", "password": "p", "extra": {"auth_type": "basic_http"}}, "basic_http needs a login"),
        ({"host": "h.org", "password": "p", "extra": {"auth_type": "netrc"}}, "netrc needs a login"),
        ({"host": "h.org", "password": "p", "extra": {"auth_type": "oauth"}}, "auth_type must be one of"),
        (
            {"host": "*.h.org", "login": "me", "password": "p", "extra": {"auth_type": "netrc"}},
            "a netrc entry needs one host",
        ),
        (
            {"host": "h.org", "login": "me", "password": "p w", "extra": {"auth_type": "netrc"}},
            "netrc cannot hold a login or password with whitespace",
        ),
    ],
)
def test_invalid_connections(connection, fields, error: str) -> None:
    connection("bad", **fields)
    with pytest.raises(AirflowException, match=error):
        PixiHook("bad").get_credentials()


def test_a_missing_connection_fails() -> None:
    with pytest.raises(AirflowNotFoundException, match="no_such_connection"), pixi_auth_env("no_such_connection", {}):
        pass


def test_two_connections_for_one_host_fail(connection) -> None:
    connection("one", host="repo.prefix.dev", password="a")
    connection("two", host="https://repo.prefix.dev/other", password="b")
    error = "'one' and 'two' both hold credentials for repo.prefix.dev"
    with pytest.raises(AirflowException, match=error), pixi_auth_env(["one", "two"], {}):
        pass


def read_auth_file():
    import os

    with open(os.environ["RATTLER_AUTH_FILE"]) as f:
        return {"path": os.environ["RATTLER_AUTH_FILE"], "content": f.read()}


def test_the_run_sees_the_credentials_file_which_is_removed_afterwards(fake_pixi, connection, home, monkeypatch):
    monkeypatch.setenv("HOME", str(home))
    connection("channel", host="repo.prefix.dev", password="token")
    op = PixiOperator(
        task_id="t",
        python_callable=read_auth_file,
        pixi_conn_id="channel",
        pixi_binary=str(fake_pixi.path),
        **INLINE,
    )
    result = op.execute({"ti": MagicMock()})
    assert json.loads(result["content"]) == {"repo.prefix.dev": {"BearerToken": "token"}}
    assert not os.path.exists(result["path"])


def test_the_credentials_file_is_removed_when_the_run_fails(fake_pixi, connection, home, monkeypatch) -> None:
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("FAKE_PIXI_FAIL", "1")
    connection("channel", host="repo.prefix.dev", password="token")
    op = PixiOperator(
        task_id="t",
        python_callable=read_auth_file,
        pixi_conn_id="channel",
        pixi_binary=str(fake_pixi.path),
        **INLINE,
    )
    seen: list[dict[str, str]] = []

    @contextlib.contextmanager
    def recording(conn_ids, env):
        with pixi_auth_env(conn_ids, env) as auth:
            seen.append(dict(auth))
            yield auth

    with (
        patch("airflow.providers.pixi.operators.pixi.pixi_auth_env", recording),
        pytest.raises(AirflowException, match="pixi run exited with code 1"),
    ):
        op.execute({"ti": MagicMock()})
    assert os.path.isabs(seen[0]["RATTLER_AUTH_FILE"])
    assert not os.path.exists(seen[0]["RATTLER_AUTH_FILE"])


def test_ui_field_behaviour_is_valid_for_airflow() -> None:
    from airflow.providers_manager import ProvidersManager

    assert PixiHook.get_ui_field_behaviour() is UI_FIELD_BEHAVIOUR
    ProvidersManager()._customized_form_fields_schema_validator.validate(UI_FIELD_BEHAVIOUR)
