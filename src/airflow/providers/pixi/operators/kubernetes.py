"""Run a Python callable inside a Pixi environment in a Kubernetes pod.

Requires ``pip install "apache-airflow-providers-pixi[cncf.kubernetes]"``.
"""

from __future__ import annotations

import base64
import json
import pickle
import shlex
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar

from kubernetes.client import models as k8s

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults
from airflow.providers.pixi.exceptions import PixiCallableError
from airflow.providers.pixi.operators.container import (
    DEFAULT_CONTAINER_PLATFORMS,
    DEFAULT_IMAGE,
    MAX_ENV_VALUE_BYTES,
    BasePixiContainerOperator,
)
from airflow.providers.pixi.operators.pixi import BasePixiPythonOperator
from airflow.providers.pixi.runtime.runner import TERMINATION_KEY, TERMINATION_LOG_ENV
from airflow.providers.pixi.utils.compat import AirflowException, AirflowSkipException

__all__ = ("DEFAULT_IMAGE", "DEFAULT_POD_PLATFORMS", "MAX_ENV_VALUE_BYTES", "PixiKubernetesPodOperator")

DEFAULT_POD_PLATFORMS = DEFAULT_CONTAINER_PLATFORMS
"""The platforms of an inline manifest without ``platforms``: those of the Linux nodes a pod can land on."""


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


class PixiKubernetesPodOperator(BasePixiContainerOperator, KubernetesPodOperator):
    """Run a Python callable inside a Pixi environment in a Kubernetes pod.

    Accepts every [`BasePixiPythonOperator`][airflow.providers.pixi.operators.pixi.BasePixiPythonOperator] argument and
    every ``KubernetesPodOperator`` argument except ``cmds`` and ``arguments``, which run the callable. Paths
    (``pixi_project_path``, ``pixi_toml_path``, ``env_cache_path``) are paths in the pod, a relative one relative to the
    image's working directory, and ``pixi_binary`` is the pixi executable of the image.

    :param image: an image with pixi [`MIN_PIXI_VERSION`][airflow.providers.pixi.utils.pixi.MIN_PIXI_VERSION] or newer,
        GNU ``sort`` and ``base64``, and the project for ``pixi_project_path``. Default
        [`DEFAULT_IMAGE`][DEFAULT_IMAGE].

    The function's source, its arguments, the task context and an inline manifest reach the pod in environment
    variables, so they are visible in the pod's spec, and each must stay under
    [`MAX_ENV_VALUE_BYTES`][MAX_ENV_VALUE_BYTES]. The return value comes back through the XCom sidecar (``do_xcom_push``
    defaults to ``True`` here), also with ``deferrable=True``. An inline manifest without ``platforms`` is solved for
    [`DEFAULT_POD_PLATFORMS`][DEFAULT_POD_PLATFORMS], in the pod on each run; mount a volume at ``env_cache_path`` to
    reuse it. An exception raised by the callable fails the task with
    [`PixiCallableError`][airflow.providers.pixi.exceptions.PixiCallableError].
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

    def pod_script(self) -> str:
        """Return the shell script the pod runs: check pixi, write the inputs, run the callable, write the XCom."""
        finish = ['[ "$status" -eq 0 ] || exit "$status"', self.missing_result_check()]
        if self.do_xcom_push:
            xcom_dir = shlex.quote(self.xcom_dir)
            finish.append(f'mkdir -p {xcom_dir} && cp "$work/output" {xcom_dir}/return.json')
        return self.container_script(finish, json_output=True)

    def pod_env_vars(self, context: Mapping[str, Any] | None = None) -> list[k8s.V1EnvVar]:
        """Return the environment variables that carry the callable, its arguments and an inline manifest.

        :param context: the task context, of which the JSON-safe part reaches the callable.
        :raises AirflowException: if the input or the manifest is longer than
            [`MAX_ENV_VALUE_BYTES`][MAX_ENV_VALUE_BYTES], so the task fails before the pod is created.
        """
        env = {**self.container_env(context), TERMINATION_LOG_ENV: self.termination_message_path}
        return [k8s.V1EnvVar(name=name, value=value) for name, value in env.items()]

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
