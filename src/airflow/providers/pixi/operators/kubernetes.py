"""Run a Python callable inside a Pixi environment in a Kubernetes pod.

Requires ``pip install "apache-airflow-providers-pixi[cncf.kubernetes]"``.
"""

from __future__ import annotations

import base64
import hashlib
import json
import pickle
import posixpath
import shlex
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults
from kubernetes.client import models as k8s

from airflow.providers.pixi.exceptions import PixiCallableError
from airflow.providers.pixi.operators.pixi import BasePixiPythonOperator
from airflow.providers.pixi.runtime.runner import TERMINATION_KEY, TERMINATION_LOG_ENV
from airflow.providers.pixi.utils.compat import AirflowException, AirflowSkipException
from airflow.providers.pixi.utils.pixi import MIN_PIXI_VERSION
from airflow.providers.pixi.utils.source import RUNNER_SCRIPT

DEFAULT_IMAGE = f"ghcr.io/prefix-dev/pixi:{MIN_PIXI_VERSION}"
"""The official pixi image in the minimum supported version."""

DEFAULT_POD_PLATFORMS = ("linux-64", "linux-aarch64")
"""The platforms of an inline manifest without ``platforms``: those of the Linux nodes a pod can land on."""

MAX_ENV_VALUE_BYTES = 120 * 1024
"""The largest value one of the pod's environment variables may have.

Linux refuses to start a process with an environment string longer than ``MAX_ARG_STRLEN`` (128 KiB), so the
pod would fail to start; the margin leaves room for the variable's name.
"""

_INPUT_ENV = "PIXI_AIRFLOW_INPUT"
_MANIFEST_ENV = "PIXI_AIRFLOW_MANIFEST"
_RUNNER_ENV = "PIXI_AIRFLOW_RUNNER"


def _env_var_list(env_vars: Any) -> list[k8s.V1EnvVar]:
    """Return the ``env_vars`` of ``KubernetesPodOperator``, a dict or a list of ``V1EnvVar``, as a list.

    :raises TypeError: for anything else, such as the string a templated ``env_vars`` renders to without
        ``render_template_as_native_obj``, which would otherwise be split into one variable per character.
    """
    if env_vars is None:
        return []
    if isinstance(env_vars, Mapping):
        return [k8s.V1EnvVar(name=name, value=value) for name, value in env_vars.items()]
    if isinstance(env_vars, (list, tuple)):
        others = [e for e in env_vars if not isinstance(e, k8s.V1EnvVar)]
        if not others:
            return list(env_vars)
        given = f"{type(env_vars).__name__} of {type(others[0]).__name__}"
    else:
        given = type(env_vars).__name__
    raise TypeError(f"env_vars must be a dict of names and values or a list of k8s.V1EnvVar, not {given}")


def _pod_path(path: str) -> str:
    """Return a shell word for ``path`` in the pod: as it is if absolute, else under the image's working directory."""
    return shlex.quote(path) if posixpath.isabs(path) else f'"$PWD"/{shlex.quote(path)}'


class PixiKubernetesPodOperator(BasePixiPythonOperator, KubernetesPodOperator):
    """Run a Python callable inside a Pixi environment in a Kubernetes pod.

    Accepts every :class:`~airflow.providers.pixi.operators.pixi.BasePixiPythonOperator` argument and every
    ``KubernetesPodOperator`` argument except ``cmds`` and ``arguments``, which run the callable. Paths
    (``pixi_project_path``, ``pixi_toml_path``, ``env_cache_path``) are paths in the pod, a relative one relative
    to the image's working directory, and ``pixi_binary`` is the pixi executable of the image.

    :param image: an image with pixi :data:`~airflow.providers.pixi.utils.pixi.MIN_PIXI_VERSION` or newer, GNU
        ``sort`` and ``base64``, and the project for ``pixi_project_path``. Default :data:`DEFAULT_IMAGE`.

    The function's source, its arguments, the task context and an inline manifest reach the pod in environment
    variables, so they are visible in the pod's spec, and each must stay under :data:`MAX_ENV_VALUE_BYTES`. The
    return value comes back through the XCom sidecar (``do_xcom_push`` defaults to ``True`` here), also with
    ``deferrable=True``. An inline manifest without ``platforms`` is solved for :data:`DEFAULT_POD_PLATFORMS`, in
    the pod on each run; mount a volume at ``env_cache_path`` to reuse it. An exception raised by the callable
    fails the task with :class:`~airflow.providers.pixi.exceptions.PixiCallableError`.
    """

    template_fields: Sequence[str] = tuple(
        dict.fromkeys(
            [
                *BasePixiPythonOperator.template_fields,
                *(f for f in KubernetesPodOperator.template_fields if f not in ("cmds", "arguments")),
            ]
        )
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **KubernetesPodOperator.template_fields_renderers,
        **BasePixiPythonOperator.template_fields_renderers,
    }
    xcom_dir = PodDefaults.XCOM_MOUNT_PATH
    """Where the ``KubernetesPodOperator`` sidecar reads ``return.json``: the path it mounts its volume at."""
    termination_message_path = "/dev/termination-log"
    """Where Kubernetes reads the container's termination message, its default ``terminationMessagePath``."""

    def __init__(self, *, image: str | None = DEFAULT_IMAGE, do_xcom_push: bool = True, **kwargs: Any) -> None:
        super().__init__(image=image, do_xcom_push=do_xcom_push, **kwargs)

    def default_platforms(self) -> list[str]:
        """Return :data:`DEFAULT_POD_PLATFORMS`: the pod runs on a Linux node, not on the worker's platform."""
        return list(DEFAULT_POD_PLATFORMS)

    def pod_script(self) -> str:
        """Return the shell script the pod runs: check pixi, write the inputs, run the callable."""
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
            f'printf %s "${_INPUT_ENV}" | base64 -d > "$work/input"',
        ]
        if self.pixi_project_path is not None:
            self._check_project_manifest(self.pixi_project_path)
            lines += [f"manifest={_pod_path(self.pixi_project_path)}", 'cd "$manifest"']
        elif self.pixi_toml_path is not None:
            self._check_project_manifest(self.pixi_toml_path)
            directory = posixpath.dirname(self.pixi_toml_path) or "."
            lines += [f"manifest={_pod_path(self.pixi_toml_path)}", f"cd {shlex.quote(directory)}"]
        else:
            if self.env_cache_path:
                digest = hashlib.sha256(self.inline_manifest_toml().encode()).hexdigest()[:16]
                lines.append(f"dir={_pod_path(posixpath.join(self.env_cache_path, f'pixi-{digest}'))}")
            else:
                lines.append('dir="$work/env"')
            lines += [
                'mkdir -p "$dir"',
                'if [ ! -f "$dir/pixi.toml" ]; then',
                f'  printf %s "${_MANIFEST_ENV}" | base64 -d > "$dir/.pixi.toml.$$"',
                '  mv "$dir/.pixi.toml.$$" "$dir/pixi.toml"',
                "fi",
                'manifest="$dir/pixi.toml"',
                'cd "$dir"',
            ]
        options = "".join(f" {shlex.quote(option)}" for option in self.pixi_run_options())
        run = f'{pixi} run --manifest-path "$manifest"{options} python -c "${_RUNNER_ENV}"'
        lines += [
            f'{run} {shlex.quote(self.serializer)} "$work/input" "$work/output" json',
            f'[ -f "$work/output" ] || {{ echo {shlex.quote(str(self._missing_result()))} >&2; exit 1; }}',
        ]
        if self.do_xcom_push:
            xcom_dir = shlex.quote(self.xcom_dir)
            lines.append(f'mkdir -p {xcom_dir} && cp "$work/output" {xcom_dir}/return.json')
        return "\n".join(lines) + "\n"

    def pod_env_vars(self, context: Mapping[str, Any] | None = None) -> list[k8s.V1EnvVar]:
        """Return the environment variables that carry the callable, its arguments and an inline manifest.

        :param context: the task context, of which the JSON-safe part reaches the callable.
        :raises AirflowException: if the input or the manifest is longer than :data:`MAX_ENV_VALUE_BYTES`, so the
            task fails before the pod is created.
        """
        env = {
            _INPUT_ENV: base64.b64encode(self.callable_input(context)).decode(),
            _RUNNER_ENV: RUNNER_SCRIPT,
            TERMINATION_LOG_ENV: self.termination_message_path,
        }
        self._check_env_size(
            _INPUT_ENV,
            env[_INPUT_ENV],
            "The callable with its op_args, op_kwargs and the task context",
            "Pass smaller arguments, such as a path or URL instead of the data, or put the code in the image and use "
            "pixi_project_path with a 'module.path:callable_name' callable.",
        )
        if self.inline_manifest:
            env[_MANIFEST_ENV] = base64.b64encode(self.inline_manifest_toml().encode()).decode()
            self._check_env_size(
                _MANIFEST_ENV,
                env[_MANIFEST_ENV],
                "The inline manifest",
                "Put the environment in a project in the image and use pixi_project_path.",
            )
        return [k8s.V1EnvVar(name=name, value=value) for name, value in env.items()]

    @staticmethod
    def _check_env_size(name: str, value: str, what: str, hint: str) -> None:
        if (size := len(value.encode())) > MAX_ENV_VALUE_BYTES:
            raise AirflowException(
                f"{what} needs {size / 1024:.0f} KiB in the pod's environment variable {name}, more than the "
                f"{MAX_ENV_VALUE_BYTES // 1024} KiB this operator allows, as Linux cannot start a process with an "
                f"environment variable longer than 128 KiB. {hint}"
            )

    def _decode_result(self, result: Any) -> Any:
        if self.do_xcom_push and self.serializer == "pickle" and isinstance(result, str):
            return pickle.loads(base64.b64decode(result))
        return result

    def _pod_callable_error(self, remote_pod: k8s.V1Pod | None) -> PixiCallableError | None:
        """Return the exception for the termination message the pod runner wrote, if the callable raised."""
        statuses = getattr(getattr(remote_pod, "status", None), "container_statuses", None) or []
        status = next((s for s in statuses if s.name == self.base_container_name), None)
        terminated = status and status.state and status.state.terminated
        try:
            return self._callable_error(json.loads(terminated.message)[TERMINATION_KEY])  # type: ignore[union-attr]
        except (AttributeError, KeyError, TypeError, ValueError):
            return None

    def execute(self, context: Any) -> Any:
        env_vars, cmds, arguments = self.env_vars, self.cmds, self.arguments
        self.env_vars = [*_env_var_list(env_vars), *self.pod_env_vars(context)]
        self.cmds, self.arguments = ["sh", "-c", self.pod_script()], []
        try:
            result = super().execute(context)
        finally:
            self.env_vars, self.cmds, self.arguments = env_vars, cmds, arguments
        return self._decode_result(result)

    def trigger_reentry(self, context: Any, event: dict[str, Any]) -> Any:
        return self._decode_result(super().trigger_reentry(context, event))

    def cleanup(self, pod: k8s.V1Pod, remote_pod: k8s.V1Pod, *args: Any, **kwargs: Any) -> None:
        try:
            super().cleanup(pod, remote_pod, *args, **kwargs)
        except AirflowSkipException:
            raise
        except AirflowException as e:
            if (error := self._pod_callable_error(remote_pod)) is None:
                raise
            raise error from e
