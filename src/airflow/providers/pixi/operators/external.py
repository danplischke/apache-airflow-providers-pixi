"""Run a Python callable with the Python of a Pixi environment that is already installed, without pixi."""

from __future__ import annotations

import contextlib
import os
import sys
from collections.abc import Iterator, Sequence
from typing import Any

from airflow.providers.pixi.operators.pixi import BasePixiPythonOperator, PixiSubprocessMixin
from airflow.providers.pixi.utils.compat import AirflowException

__all__ = ("PixiExternalPythonOperator",)

DEFAULT_ENVIRONMENT = "default"
"""The environment pixi installs when none is named."""


def environment_prefix(workspace: str, environment: str | None = None) -> str:
    """Return the directory pixi installs ``environment`` of the workspace at ``workspace`` into.

    ``<workspace>/.pixi/envs/<environment>``, the default ``default``, unless pixi is configured to install
    environments elsewhere (``detached-environments``).
    """
    return os.path.join(workspace, ".pixi", "envs", environment or DEFAULT_ENVIRONMENT)


def environment_python(prefix: str) -> str:
    """Return the Python of the conda environment at ``prefix``."""
    if sys.platform == "win32":
        return os.path.join(prefix, "python.exe")
    return os.path.join(prefix, "bin", "python")


def _environment_path(prefix: str) -> list[str]:
    """Return the directories of the environment ``pixi run`` puts on ``PATH``."""
    if sys.platform == "win32":
        return [prefix, *(os.path.join(prefix, *d) for d in (("Library", "bin"), ("Scripts",), ("bin",)))]
    return [os.path.join(prefix, "bin")]


class PixiExternalPythonOperator(PixiSubprocessMixin, BasePixiPythonOperator):
    """Run a Python callable with the Python of a Pixi environment that is already installed, without pixi.

    The Pixi counterpart of ``ExternalPythonOperator``, for images and hosts whose environments were installed with
    ``pixi install`` when they were built. The callable runs with ``<workspace>/.pixi/envs/<environment>/bin/python``
    of the project at ``pixi_project_path`` or ``pixi_toml_path``, where pixi installs the environment, so no pixi is
    needed at run time, and nothing is solved, downloaded or installed. The run's working directory is the
    manifest's directory, and ``PATH`` and ``CONDA_PREFIX`` point at the environment; activation scripts and the
    manifest's ``[activation]`` are not applied, so set the variables they would set with ``env_vars``.

    Accepts the arguments of [`PixiOperator`][airflow.providers.pixi.operators.pixi.PixiOperator] (``python_callable``,
    ``op_args``, ``op_kwargs``, ``serializer``, ``environment``, ``env_vars``, ``env_from_variables``,
    ``env_from_connections``, ``skip_on_exit_code``, ...), except an inline manifest, which is not installed anywhere.
    ``lock_mode``, ``pixi_binary``, ``pixi_conn_id`` and the cache directory Variables have no effect. As for
    ``PixiOperator``, the worker's ``PYTHONPATH``, ``PYTHONHOME``, ``PYTHONUSERBASE`` and ``VIRTUAL_ENV`` are left
    out, an exception raised by the callable fails the task with
    [`PixiCallableError`][airflow.providers.pixi.exceptions.PixiCallableError], and the return value is the task's
    XCom.

    A missing environment fails the task: install it when the image is built, with ``pixi install --manifest-path
    <manifest> [--environment <environment>] --frozen``. Environments that pixi installs elsewhere
    (``detached-environments``) are not found.
    """

    template_fields: Sequence[str] = (*BasePixiPythonOperator.template_fields, *PixiSubprocessMixin.template_fields)
    custom_operator_name = "PixiExternalPython"
    _run_name = "The environment's Python"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.pixi_conn_id = None
        self.pixi_cache_dir_variable = None
        self.uv_cache_dir_variable = None
        self.pip_cache_dir_variable = None
        if self.inline_manifest:
            raise ValueError(
                f"{type(self).__name__} runs an environment that is already installed, which an inline manifest is "
                "not; use pixi_project_path or pixi_toml_path, or PixiOperator to install the inline manifest"
            )

    def installed_python(self, workspace: str) -> tuple[str, str]:
        """Return the prefix and the Python of the environment in ``workspace``, from the rendered ``environment``.

        :raises AirflowException: if the environment is not installed there.
        """
        environment = self._checked_environment(AirflowException)
        prefix = environment_prefix(workspace, environment)
        python = environment_python(prefix)
        if not os.path.isfile(python):
            option = f" --environment {environment}" if environment else ""
            raise AirflowException(
                f"No Python at {python}: the environment {environment or DEFAULT_ENVIRONMENT!r} of {workspace} is not "
                f"installed. Install it where the image is built, with `pixi install --manifest-path {workspace}"
                f"{option} --frozen`, or run the task with PixiOperator, which installs it."
            )
        return prefix, python

    @contextlib.contextmanager
    def python_command(self) -> Iterator[tuple[list[str], str, dict[str, str]]]:
        """Yield the environment's Python, the workspace directory, and ``PATH`` and ``CONDA_PREFIX`` for it."""
        with self.local_manifest() as (_, workspace):
            prefix, python = self.installed_python(workspace)
            path = os.pathsep.join([*_environment_path(prefix), *filter(None, [os.environ.get("PATH")])])
            yield [python], workspace, {"PATH": path, "CONDA_PREFIX": prefix}

    def execute(self, context: Any) -> Any:
        return self.run_callable(context)
