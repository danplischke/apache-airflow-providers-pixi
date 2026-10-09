"""Credentials of private conda channels and PyPI indexes for pixi, from Airflow connections of type ``pixi``.

Pixi reads conda channel credentials from the JSON file named by ``RATTLER_AUTH_FILE`` and PyPI index credentials
from the netrc file named by ``NETRC`` (https://pixi.sh/latest/deployment/authentication/). For a run,
[`pixi_auth_env`][pixi_auth_env] writes both to temporary files and removes them afterwards.
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import os
import shutil
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

from airflow.providers.pixi.utils.compat import AirflowException
from airflow.sdk import BaseHook
from airflow.sdk.log import mask_secret

__all__ = ("AUTH_TYPES", "PixiCredentials", "PixiHook", "pixi_auth_env")

AUTH_TYPES = ("auto", "bearer_token", "conda_token", "basic_http", "netrc")
"""Values of the ``auth_type`` extra of a ``pixi`` connection."""

UI_FIELD_BEHAVIOUR: dict[str, Any] = {
    "hidden_fields": ["schema", "port"],
    "relabeling": {"host": "Host", "login": "Username", "password": "Token or password"},
    "placeholders": {
        "host": "repo.prefix.dev, *.prefix.dev or artifactory.example.com",
        "login": "only for basic_http and netrc",
        "password": "the token, or the password for basic_http and netrc",
        "extra": '{"auth_type": "auto"}',
    },
}


@dataclasses.dataclass(frozen=True)
class PixiCredentials:
    """Credentials for one host, from a ``pixi`` connection.

    :param conn_id: the connection they come from.
    :param auth_type: one of [`AUTH_TYPES`][AUTH_TYPES] except ``"auto"``.
    :param host: the host name pixi looks them up by, such as ``repo.prefix.dev`` or ``*.prefix.dev``.
    :param login: the username of ``basic_http`` and ``netrc``.
    :param secret: the token or password.
    """

    conn_id: str
    auth_type: str
    host: str
    login: str | None
    secret: str = dataclasses.field(repr=False)

    def rattler_entry(self) -> dict[str, Any]:
        """Return the value for ``host`` in pixi's credentials file, for a conda channel."""
        if self.auth_type == "bearer_token":
            return {"BearerToken": self.secret}
        if self.auth_type == "conda_token":
            return {"CondaToken": self.secret}
        if self.auth_type == "basic_http":
            return {"BasicHTTP": {"username": self.login, "password": self.secret}}
        raise ValueError(f"{self.auth_type} credentials of {self.conn_id!r} do not go into the credentials file")

    def netrc_entry(self) -> str:
        """Return the netrc lines for ``host``, for a PyPI index."""
        return f"machine {self.host}\nlogin {self.login}\npassword {self.secret}\n"


def _host_name(host: str) -> str | None:
    return urlsplit(host if "://" in host else f"//{host}").hostname


class PixiHook(BaseHook):
    """Credentials of a private conda channel or PyPI index, from an Airflow connection of type ``pixi``.

    The connection's ``host`` is the host of the channel or index, such as ``repo.prefix.dev`` (a URL is reduced
    to its host name; ``*.prefix.dev`` matches every subdomain of a channel). ``password`` is the token or the
    password, and ``login`` the username for ``basic_http`` and ``netrc``. ``auth_type`` in the extra chooses how
    pixi sends them:

    - ``"auto"`` (default): ``basic_http`` if the connection has a login, otherwise ``bearer_token``.
    - ``"bearer_token"``: an ``Authorization: Bearer`` header, as prefix.dev uses.
    - ``"conda_token"``: a token in the channel URL, as anaconda.org and quetz use.
    - ``"basic_http"``: HTTP basic authentication, as Artifactory and Nexus use.
    - ``"netrc"``: a netrc entry, for a PyPI index named in ``[pypi-options]`` of a project's ``pixi.toml``.

    :param pixi_conn_id: the connection id.
    """

    conn_name_attr = "pixi_conn_id"
    default_conn_name = "pixi_default"
    conn_type = "pixi"
    hook_name = "Pixi"

    def __init__(self, pixi_conn_id: str = default_conn_name, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.pixi_conn_id = pixi_conn_id

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return the connection form's fields, for Airflow 3.1."""
        return UI_FIELD_BEHAVIOUR

    def get_conn(self) -> Any:
        """Return the connection."""
        return self.get_connection(self.pixi_conn_id)

    def get_credentials(self) -> PixiCredentials:
        """Return the connection's credentials; the token or password is masked in the task log."""
        conn = self.get_conn()
        extra = conn.extra_dejson
        auth_type = extra.get("auth_type") or extra.get("extra__pixi__auth_type") or "auto"
        where = f"Pixi connection {self.pixi_conn_id!r}"
        if auth_type not in AUTH_TYPES:
            raise AirflowException(f"{where}: auth_type must be one of {', '.join(AUTH_TYPES)}, not {auth_type!r}")
        if auth_type == "auto":
            auth_type = "basic_http" if conn.login else "bearer_token"
        host = _host_name(conn.host or "")
        if not host:
            raise AirflowException(f"{where} has no host; set it to the host of the channel or index")
        if not conn.password:
            raise AirflowException(f"{where} has no password; set it to the token or password")
        if auth_type in ("basic_http", "netrc") and not conn.login:
            raise AirflowException(f"{where}: {auth_type} needs a login")
        if auth_type == "netrc":
            if "*" in host:
                raise AirflowException(f"{where}: a netrc entry needs one host, not {host!r}")
            if any(not value.split() or len(value.split()) > 1 for value in (conn.login, conn.password)):
                raise AirflowException(f"{where}: netrc cannot hold a login or password with whitespace")
        mask_secret(conn.password)
        return PixiCredentials(
            conn_id=self.pixi_conn_id,
            auth_type=auth_type,
            host=host,
            login=conn.login or None,
            secret=conn.password,
        )


def _write_private(path: str, text: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)


def _existing_credentials(path: str) -> dict[str, Any]:
    if not os.path.isfile(path):
        return {}
    try:
        with open(path) as f:
            entries = json.load(f)
    except (OSError, ValueError) as e:
        raise AirflowException(f"Could not read the pixi credentials file {path}: {e}") from e
    if not isinstance(entries, dict):
        raise AirflowException(f"The pixi credentials file {path} does not hold a JSON object")
    return entries


def _existing_netrc(path: str) -> str:
    if not os.path.isfile(path):
        return ""
    try:
        with open(path) as f:
            text = f.read()
    except OSError as e:
        raise AirflowException(f"Could not read the netrc file {path}: {e}") from e
    return text if not text or text.endswith("\n") else text + "\n"


@contextlib.contextmanager
def pixi_auth_env(conn_ids: str | Sequence[str] | None, env: Mapping[str, str]) -> Iterator[dict[str, str]]:
    """Yield ``RATTLER_AUTH_FILE`` and ``NETRC`` for the credentials of the ``pixi`` connections ``conn_ids``.

    Each points at a temporary file, readable only by the worker's user and removed when the block exits; only
    the variables the connections need are set, and none without connections. While ``RATTLER_AUTH_FILE`` is
    set, pixi reads no other credentials, so the file also holds those of the file pixi would read otherwise:
    the one ``env`` names in ``RATTLER_AUTH_FILE``, or ``~/.rattler/credentials.json``. Likewise the netrc file
    starts with the connections' entries, followed by the one ``env`` names in ``NETRC``, or ``~/.netrc``.
    Credentials in the system keychain are not copied. In the credentials file, a connection's entry replaces
    one for the same host; netrc readers differ in which entry of a host they use, so keep a host in one place.

    :param conn_ids: a connection id, or several, each for another host.
    :param env: the environment pixi will run with, for ``HOME`` and the files it names.
    """
    ids = [conn_ids] if isinstance(conn_ids, str) else list(conn_ids or [])
    if not ids:
        yield {}
        return
    credentials = [PixiHook(conn_id).get_credentials() for conn_id in ids]
    seen: dict[tuple[bool, str], str] = {}
    for c in credentials:
        key = (c.auth_type == "netrc", c.host)
        if key in seen:
            raise AirflowException(
                f"Pixi connections {seen[key]!r} and {c.conn_id!r} both hold credentials for {c.host}"
            )
        seen[key] = c.conn_id
    home = env.get("HOME") or os.path.expanduser("~")
    rattler = [c for c in credentials if c.auth_type != "netrc"]
    netrc = [c for c in credentials if c.auth_type == "netrc"]
    directory = tempfile.mkdtemp(prefix="airflow_pixi_auth_")
    try:
        result: dict[str, str] = {}
        if rattler:
            existing = env.get("RATTLER_AUTH_FILE") or os.path.join(home, ".rattler", "credentials.json")
            entries = {**_existing_credentials(existing), **{c.host: c.rattler_entry() for c in rattler}}
            result["RATTLER_AUTH_FILE"] = os.path.join(directory, "credentials.json")
            _write_private(result["RATTLER_AUTH_FILE"], json.dumps(entries, indent=2))
        if netrc:
            existing = env.get("NETRC") or os.path.join(home, ".netrc")
            result["NETRC"] = os.path.join(directory, "netrc")
            _write_private(result["NETRC"], "".join(c.netrc_entry() for c in netrc) + _existing_netrc(existing))
        yield result
    finally:
        shutil.rmtree(directory, ignore_errors=True)
