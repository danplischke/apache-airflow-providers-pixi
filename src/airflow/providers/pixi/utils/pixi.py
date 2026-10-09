"""The pixi binary on the worker: where it is, whether it is new enough, and the platform it installs for.

Pixi is never installed here.
"""

from __future__ import annotations

import functools
import os
import platform
import re
import shutil
import subprocess
import sys

from packaging.version import InvalidVersion, Version

from airflow.providers.pixi.utils.compat import AirflowException

MIN_PIXI_VERSION = Version("0.81.0")
"""Oldest pixi this provider supports. The integration and system tests run against it."""

_PIXI_VERSION_OUTPUT = re.compile(r"\b(\d+\.\d+\.\d+\S*)")

_PIXI_PLATFORMS = {
    ("linux", "x86_64"): "linux-64",
    ("linux", "amd64"): "linux-64",
    ("linux", "aarch64"): "linux-aarch64",
    ("linux", "arm64"): "linux-aarch64",
    ("darwin", "x86_64"): "osx-64",
    ("darwin", "arm64"): "osx-arm64",
    ("win32", "amd64"): "win-64",
    ("win32", "x86_64"): "win-64",
}


def local_platform() -> str:
    """Return the pixi platform of this machine, such as ``linux-64`` or ``osx-arm64``."""
    system = "linux" if sys.platform.startswith("linux") else sys.platform
    machine = platform.machine()
    try:
        return _PIXI_PLATFORMS[system, machine.lower()]
    except KeyError:
        raise AirflowException(
            f"No pixi platform is known for {sys.platform} on {machine or 'an unknown CPU'}; "
            "pass platforms explicitly, for example platforms=['linux-64']"
        ) from None


def pixi_version(path: str) -> Version:
    """Return the version of the pixi binary at ``path``, asked once per binary file."""
    return _cached_pixi_version(path, os.stat(path).st_mtime_ns)


@functools.cache
def _cached_pixi_version(path: str, mtime_ns: int) -> Version:
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
