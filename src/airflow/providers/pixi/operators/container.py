"""The base class of the operators that run a Python callable inside a Pixi environment in a container.

[`PixiKubernetesPodOperator`][airflow.providers.pixi.operators.kubernetes.PixiKubernetesPodOperator] and
[`PixiDockerOperator`][airflow.providers.pixi.operators.docker.PixiDockerOperator] start a container that runs the
shell script of [`BasePixiContainerOperator`][BasePixiContainerOperator]: it checks the image's pixi, writes the
callable's input from an environment variable and runs it with ``pixi run``. Each operator then hands the result
back its own way.
"""

from __future__ import annotations

import base64
import hashlib
import posixpath
import shlex
from collections.abc import Mapping, Sequence
from typing import Any

from airflow.providers.pixi.operators.pixi import BasePixiPythonOperator
from airflow.providers.pixi.utils.compat import AirflowException
from airflow.providers.pixi.utils.pixi import MIN_PIXI_VERSION
from airflow.providers.pixi.utils.source import RUNNER_SCRIPT

__all__ = (
    "DEFAULT_CONTAINER_PLATFORMS",
    "DEFAULT_IMAGE",
    "MAX_ENV_VALUE_BYTES",
    "BasePixiContainerOperator",
)

DEFAULT_IMAGE = f"ghcr.io/prefix-dev/pixi:{MIN_PIXI_VERSION}"
"""The official pixi image in the minimum supported version."""

DEFAULT_CONTAINER_PLATFORMS = ("linux-64", "linux-aarch64")
"""The platforms of an inline manifest without ``platforms``: those of the Linux hosts a container can run on."""

MAX_ENV_VALUE_BYTES = 120 * 1024
"""The largest value one of the container's environment variables may have.

Linux refuses to start a process with an environment string longer than ``MAX_ARG_STRLEN`` (128 KiB), so the
container would fail to start; the margin leaves room for the variable's name.
"""

INPUT_ENV = "PIXI_AIRFLOW_INPUT"
MANIFEST_ENV = "PIXI_AIRFLOW_MANIFEST"
RUNNER_ENV = "PIXI_AIRFLOW_RUNNER"


def container_path(path: str) -> str:
    """Return a shell word for ``path`` in the container: as it is if absolute, else under the image's working directory."""
    return shlex.quote(path) if posixpath.isabs(path) else f'"$PWD"/{shlex.quote(path)}'


class BasePixiContainerOperator(BasePixiPythonOperator):
    """Base class of the operators that run a Python callable inside a Pixi environment in a container.

    Accepts every [`BasePixiPythonOperator`][airflow.providers.pixi.operators.pixi.BasePixiPythonOperator] argument.
    Paths (``pixi_project_path``, ``pixi_toml_path``, ``env_cache_path``) are paths in the container, a relative one
    relative to the image's working directory, and ``pixi_binary`` is the pixi executable of the image. An inline
    manifest without ``platforms`` is solved for [`DEFAULT_CONTAINER_PLATFORMS`][DEFAULT_CONTAINER_PLATFORMS], in the
    container on each run; mount a volume at ``env_cache_path`` to reuse it.

    The function's source, its arguments, the task context and an inline manifest reach the container in environment
    variables ([`container_env`][container_env]), each of which must stay under
    [`MAX_ENV_VALUE_BYTES`][MAX_ENV_VALUE_BYTES]. The container runs [`container_script`][container_script].
    """

    def default_platforms(self) -> list[str]:
        """Return [`DEFAULT_CONTAINER_PLATFORMS`][DEFAULT_CONTAINER_PLATFORMS]: containers run on Linux, not on the worker."""
        return list(DEFAULT_CONTAINER_PLATFORMS)

    def container_script(self, finish: Sequence[str], *, json_output: bool = False) -> str:
        """Return the shell script the container runs: check pixi, write the inputs, run the callable.

        :param finish: the shell lines that follow the run, in the directory of the manifest. ``$status`` holds the
            exit code of ``pixi run``, ``$work/output`` the result, if the callable returned, and
            ``$work/output.error`` the description of the exception, if it raised.
        :param json_output: write a pickled result as a base64 JSON string, for a JSON XCom file.
        """
        min_version = shlex.quote(str(MIN_PIXI_VERSION))
        pixi = shlex.quote(self.pixi_binary)
        lines = [
            "set -eu",
            f'version="$({pixi} --version)"',
            'version="${version##* }"',
            f'if [ "$(printf "%s\\n%s\\n" {min_version} "$version" | sort -V | head -n 1)" != {min_version} ]; then',
            f'  echo "pixi $version in the image is older than {MIN_PIXI_VERSION}, which this provider needs" >&2',
            "  exit 1",
            "fi",
            'work="$(mktemp -d)"',
            f'printf %s "${INPUT_ENV}" | base64 -d > "$work/input"',
        ]
        if self.pixi_project_path is not None:
            self._check_project_manifest(self.pixi_project_path)
            lines += [f"manifest={container_path(self.pixi_project_path)}", 'cd "$manifest"']
        elif self.pixi_toml_path is not None:
            self._check_project_manifest(self.pixi_toml_path)
            directory = posixpath.dirname(self.pixi_toml_path) or "."
            lines += [f"manifest={container_path(self.pixi_toml_path)}", f"cd {shlex.quote(directory)}"]
        else:
            if self.env_cache_path:
                digest = hashlib.sha256(self.inline_manifest_toml().encode()).hexdigest()[:16]
                lines.append(f"dir={container_path(posixpath.join(self.env_cache_path, f'pixi-{digest}'))}")
            else:
                lines.append('dir="$work/env"')
            lines += [
                'mkdir -p "$dir"',
                'if [ ! -f "$dir/pixi.toml" ]; then',
                f'  printf %s "${MANIFEST_ENV}" | base64 -d > "$dir/.pixi.toml.$$"',
                '  mv "$dir/.pixi.toml.$$" "$dir/pixi.toml"',
                "fi",
                'manifest="$dir/pixi.toml"',
                'cd "$dir"',
            ]
        options = "".join(f" {shlex.quote(option)}" for option in self.pixi_run_options())
        run = f'{pixi} run --manifest-path "$manifest"{options} python -c "${RUNNER_ENV}"'
        output_format = " json" if json_output else ""
        lines += [
            "status=0",
            f'{run} {shlex.quote(self.serializer)} "$work/input" "$work/output"{output_format} || status=$?',
            *finish,
        ]
        return "\n".join(lines) + "\n"

    def missing_result_check(self) -> str:
        """Return the shell line that fails the container when the callable exited without returning."""
        return f'[ -f "$work/output" ] || {{ echo {shlex.quote(str(self._missing_result()))} >&2; exit 1; }}'

    def container_env(self, context: Mapping[str, Any] | None = None) -> dict[str, str]:
        """Return the environment variables that carry the callable, its arguments and an inline manifest.

        :param context: the task context, of which the JSON-safe part reaches the callable.
        :raises AirflowException: if the input or the manifest is longer than
            [`MAX_ENV_VALUE_BYTES`][MAX_ENV_VALUE_BYTES], so the task fails before the container is created.
        """
        env = {
            INPUT_ENV: base64.b64encode(self.callable_input(context)).decode(),
            RUNNER_ENV: RUNNER_SCRIPT,
        }
        self._check_env_size(
            INPUT_ENV,
            env[INPUT_ENV],
            "The callable with its op_args, op_kwargs and the task context",
            "Pass smaller arguments, such as a path or URL instead of the data, or put the code in the image and use "
            "pixi_project_path with a 'module.path:callable_name' callable.",
        )
        if self.inline_manifest:
            env[MANIFEST_ENV] = base64.b64encode(self.inline_manifest_toml().encode()).decode()
            self._check_env_size(
                MANIFEST_ENV,
                env[MANIFEST_ENV],
                "The inline manifest",
                "Put the environment in a project in the image and use pixi_project_path.",
            )
        return env

    @staticmethod
    def _check_env_size(name: str, value: str, what: str, hint: str) -> None:
        if (size := len(value.encode())) > MAX_ENV_VALUE_BYTES:
            raise AirflowException(
                f"{what} needs {size / 1024:.0f} KiB in the container's environment variable {name}, more than the "
                f"{MAX_ENV_VALUE_BYTES // 1024} KiB this operator allows, as Linux cannot start a process with an "
                f"environment variable longer than 128 KiB. {hint}"
            )
