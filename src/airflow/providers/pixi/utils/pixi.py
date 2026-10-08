"""The pixi binary on the worker: where it is and whether it is new enough. Pixi is never installed here."""

from __future__ import annotations

import functools
import os
import re
import shutil
import subprocess

from airflow.exceptions import AirflowException
from packaging.version import InvalidVersion, Version

MIN_PIXI_VERSION = Version("0.81.0")
"""Oldest pixi this provider supports. The integration and system tests run against it."""

_PIXI_VERSION_OUTPUT = re.compile(r"\b(\d+\.\d+\.\d+\S*)")


def pixi_version(path: str) -> Version:
    """Return the version of the pixi binary at ``path``, asked once per binary file."""
    return _cached_pixi_version(path, os.stat(path).st_mtime_ns)


@functools.cache
def _cached_pixi_version(path: str, mtime_ns: int) -> Version:
    # mtime_ns is part of the key, so a binary replaced in place, e.g. by `pixi self-update`, is asked again
    try:
        proc = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=30, check=False)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise AirflowException(f"Could not run {path} --version: {e}") from e
    match = _PIXI_VERSION_OUTPUT.search(proc.stdout)
    try:
        if proc.returncode == 0 and match:
            return Version(match.group(1))
    except InvalidVersion:
        pass
    raise AirflowException(
        f"Could not read the pixi version from {path} --version: {(proc.stdout + proc.stderr).strip()}"
    )


def resolve_pixi(pixi_binary: str) -> str:
    """Return the path of ``pixi_binary``; fail if it is missing or older than :data:`MIN_PIXI_VERSION`.

    Pixi is never installed by the provider: it has to be part of the worker's environment.
    """
    resolved = shutil.which(pixi_binary)
    if resolved is None:
        raise AirflowException(
            f"Pixi binary {pixi_binary!r} not found on PATH. Install pixi {MIN_PIXI_VERSION} or newer on the "
            "workers (https://pixi.sh/latest/installation/), or pass its path as pixi_binary."
        )
    version = pixi_version(resolved)
    if version < MIN_PIXI_VERSION:
        raise AirflowException(
            f"{resolved} is pixi {version}, but this provider needs pixi {MIN_PIXI_VERSION} or newer. "
            "Upgrade it on the workers, for example with `pixi self-update`."
        )
    return resolved
