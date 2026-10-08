"""Run a Python callable inside a Pixi environment in a Kubernetes pod.

Requires ``pip install "apache-airflow-providers-pixi[cncf.kubernetes]"``.
"""

from __future__ import annotations

import base64
import hashlib
import pickle
import posixpath
import shlex
from collections.abc import Sequence
from typing import Any, ClassVar

from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import convert_env_vars
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

from airflow.providers.pixi.operators.pixi import BasePixiPythonOperator
from airflow.providers.pixi.utils.pixi import MIN_PIXI_VERSION
from airflow.providers.pixi.utils.source import RUNNER_SCRIPT

DEFAULT_IMAGE = f"ghcr.io/prefix-dev/pixi:{MIN_PIXI_VERSION}"
"""The official pixi image in the minimum supported version."""

_INPUT_ENV = "PIXI_AIRFLOW_INPUT"
_MANIFEST_ENV = "PIXI_AIRFLOW_MANIFEST"
_RUNNER_ENV = "PIXI_AIRFLOW_RUNNER"


class PixiKubernetesPodOperator(BasePixiPythonOperator, KubernetesPodOperator):
    """Run a Python callable inside a Pixi environment in a Kubernetes pod.

    Accepts every :class:`~airflow.providers.pixi.operators.pixi.BasePixiPythonOperator` argument and every
    ``KubernetesPodOperator`` argument except ``cmds`` and ``arguments``, which run the callable. Paths
    (``pixi_project_path``, ``pixi_toml_path``, ``env_cache_path``) are paths in the pod, and ``pixi_binary``
    is the pixi executable of the image.

    :param image: an image with pixi :data:`~airflow.providers.pixi.utils.pixi.MIN_PIXI_VERSION` or newer, GNU
        ``sort`` and ``base64``, and the project for ``pixi_project_path``. Default :data:`DEFAULT_IMAGE`.

    The function's source, its arguments and an inline manifest reach the pod in environment variables;
    the return value comes back through the XCom sidecar (``do_xcom_push`` defaults to ``True`` here). An inline
    environment is solved in the pod on each run; mount a volume at ``env_cache_path`` to reuse it.
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
    xcom_dir = "/airflow/xcom"
    """Where the ``KubernetesPodOperator`` sidecar reads ``return.json``."""

    def __init__(self, *, image: str | None = DEFAULT_IMAGE, do_xcom_push: bool = True, **kwargs: Any) -> None:
        # unlike KubernetesPodOperator, the return value is pushed by default, as for every Python operator
        super().__init__(image=image, do_xcom_push=do_xcom_push, **kwargs)

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
            lines += [f"manifest={shlex.quote(self.pixi_project_path)}", 'cd "$manifest"']
        elif self.pixi_toml_path is not None:
            self._check_project_manifest(self.pixi_toml_path)
            directory = posixpath.dirname(self.pixi_toml_path) or "."
            lines += [f"manifest={shlex.quote(self.pixi_toml_path)}", f"cd {shlex.quote(directory)}"]
        else:
            if self.env_cache_path:
                digest = hashlib.sha256(self.inline_manifest_toml().encode()).hexdigest()[:16]
                lines.append(f"dir={shlex.quote(posixpath.join(self.env_cache_path, f'pixi-{digest}'))}")
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
        environment = f" --environment {shlex.quote(self.environment)}" if self.environment else ""
        run = f'{pixi} run --manifest-path "$manifest"{environment} python -c "${_RUNNER_ENV}"'
        lines += [
            f'{run} {shlex.quote(self.serializer)} "$work/input" "$work/output" json',
            f'[ -f "$work/output" ] || {{ echo {shlex.quote(str(self._missing_result()))} >&2; exit 1; }}',
        ]
        if self.do_xcom_push:
            xcom_dir = shlex.quote(self.xcom_dir)
            lines.append(f'mkdir -p {xcom_dir} && cp "$work/output" {xcom_dir}/return.json')
        return "\n".join(lines) + "\n"

    def pod_env_vars(self) -> list[k8s.V1EnvVar]:
        """Return the environment variables that carry the callable, its arguments and an inline manifest."""
        env = [
            k8s.V1EnvVar(name=_INPUT_ENV, value=base64.b64encode(self.callable_input()).decode()),
            k8s.V1EnvVar(name=_RUNNER_ENV, value=RUNNER_SCRIPT),
        ]
        if self.inline_manifest:
            manifest = base64.b64encode(self.inline_manifest_toml().encode()).decode()
            env.append(k8s.V1EnvVar(name=_MANIFEST_ENV, value=manifest))
        return env

    def execute(self, context: Any) -> Any:
        env_vars, cmds, arguments = self.env_vars, self.cmds, self.arguments
        self.env_vars = [*convert_env_vars(env_vars or []), *self.pod_env_vars()]
        self.cmds, self.arguments = ["sh", "-c", self.pod_script()], []
        try:
            result = super().execute(context)
        finally:
            self.env_vars, self.cmds, self.arguments = env_vars, cmds, arguments
        if self.do_xcom_push and self.serializer == "pickle" and isinstance(result, str):
            return pickle.loads(base64.b64decode(result))
        return result
