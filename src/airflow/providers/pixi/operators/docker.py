"""Run a Python callable inside a Pixi environment in a Docker container.

Requires ``pip install "apache-airflow-providers-pixi[docker]"``.
"""

from __future__ import annotations

import dataclasses
import json
import pickle
import shlex
from collections.abc import Mapping, Sequence
from typing import IO, Any, ClassVar

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.pixi.operators.container import (
    DEFAULT_CONTAINER_PLATFORMS,
    DEFAULT_IMAGE,
    MAX_ENV_VALUE_BYTES,
    BasePixiContainerOperator,
)
from airflow.providers.pixi.operators.pixi import BasePixiPythonOperator
from airflow.providers.pixi.runtime.runner import ERROR_SUFFIX
from airflow.providers.pixi.utils.compat import AirflowException, AirflowSkipException

__all__ = ("DEFAULT_CONTAINER_PLATFORMS", "DEFAULT_IMAGE", "MAX_ENV_VALUE_BYTES", "PixiDockerOperator")

_RESULT = b"ok"
_ERROR = b"error"

_SET_BY_THE_OPERATOR = ("command", "entrypoint", "retrieve_output", "retrieve_output_path")


@dataclasses.dataclass(frozen=True)
class _Result:
    """The callable returned ``value``."""

    value: Any


@dataclasses.dataclass(frozen=True)
class _CallableFailed:
    """The callable raised: the runner's description of the exception, and the exit code of ``pixi run``."""

    error: dict[str, str]
    exit_code: int


@dataclasses.dataclass(frozen=True)
class _ResultReader:
    """Reads the result file for ``DockerOperator``, which calls ``load`` of its ``pickling_library``.

    The file starts with a line saying whether it holds the result, in the ``serializer``'s format, or, after the exit
    code of ``pixi run``, the description of the exception the callable raised.
    """

    serializer: str

    def load(self, file: IO[bytes]) -> _Result | _CallableFailed:
        kind, *rest = file.readline().split()
        data = file.read()
        if kind == _ERROR and len(rest) == 1 and rest[0].isdigit():
            return _CallableFailed(json.loads(data), int(rest[0]))
        if kind != _RESULT or rest:
            raise ValueError(f"the result file does not start with {_RESULT!r} or {_ERROR!r} and an exit code")
        return _Result(pickle.loads(data) if self.serializer == "pickle" else json.loads(data))


class PixiDockerOperator(BasePixiContainerOperator, DockerOperator):
    """Run a Python callable inside a Pixi environment in a Docker container.

    Accepts every [`BasePixiPythonOperator`][airflow.providers.pixi.operators.pixi.BasePixiPythonOperator] argument and
    every ``DockerOperator`` argument (``docker_url``, ``docker_conn_id``, ``mounts``, ``network_mode``,
    ``private_environment``, ``auto_remove``, ``skip_on_exit_code``, ...) except ``command``, ``entrypoint``,
    ``retrieve_output`` and ``retrieve_output_path``, which run the callable and bring back its result, and
    ``environment``, which is the Pixi environment here. Paths (``pixi_project_path``, ``pixi_toml_path``,
    ``env_cache_path``) are paths in the container, a relative one relative to the image's working directory, and
    ``pixi_binary`` is the pixi executable of the image.

    :param image: an image with pixi [`MIN_PIXI_VERSION`][airflow.providers.pixi.utils.pixi.MIN_PIXI_VERSION] or newer,
        ``sh``, GNU ``sort`` and ``base64``, and the project for ``pixi_project_path``. Default
        [`DEFAULT_IMAGE`][airflow.providers.pixi.operators.container.DEFAULT_IMAGE].
    :param env_vars: environment variables of the container (templated), what ``environment`` is for
        ``DockerOperator``. ``private_environment`` sets more, which are not templated or shown in the UI.
    :param env_file: the content of a ``.env`` file with more variables (templated). Unlike for ``DockerOperator``, a
        path ending in ``.env`` is not read as a template file, as that would also apply to ``op_args`` and
        ``op_kwargs``; read the file with ``"{% include 'app.env' %}"``.

    The function's source, its arguments, the task context and an inline manifest reach the container in environment
    variables, which ``docker inspect`` shows, and each must stay under
    [`MAX_ENV_VALUE_BYTES`][airflow.providers.pixi.operators.container.MAX_ENV_VALUE_BYTES]. The return value is
    copied out of the container to become the task's XCom, so it does not pass through the log. An inline manifest
    without ``platforms`` is solved for
    [`DEFAULT_CONTAINER_PLATFORMS`][airflow.providers.pixi.operators.container.DEFAULT_CONTAINER_PLATFORMS], in the
    container on each run; mount a volume at ``env_cache_path`` to reuse it. An exception raised by the callable fails
    the task with [`PixiCallableError`][airflow.providers.pixi.exceptions.PixiCallableError], or skips it if
    ``skip_on_exit_code`` holds the exit code it causes, as for
    [`PixiOperator`][airflow.providers.pixi.operators.pixi.PixiOperator].
    """

    template_fields: Sequence[str] = tuple(
        dict.fromkeys(
            [
                *BasePixiPythonOperator.template_fields,
                *(f for f in DockerOperator.template_fields if f != "command"),
                "env_vars",
            ]
        )
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DockerOperator.template_fields_renderers,
        **BasePixiPythonOperator.template_fields_renderers,
        "env_vars": "json",
    }
    template_ext: Sequence[str] = ()
    custom_operator_name = "PixiDocker"
    result_path = "/tmp/pixi-airflow-result"
    """Where in the container the script writes the result, which is copied out after the container exits."""

    def __init__(
        self,
        *,
        image: str = DEFAULT_IMAGE,
        env_vars: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        if given := [name for name in _SET_BY_THE_OPERATOR if kwargs.get(name) is not None]:
            raise ValueError(
                f"{type(self).__name__} sets {', '.join(given)} itself, to run the callable and bring back its result"
            )
        super().__init__(image=image, retrieve_output=True, retrieve_output_path=self.result_path, **kwargs)
        self.env_vars = env_vars

    @property
    def pickling_library(self) -> _ResultReader:
        """What ``DockerOperator`` reads the file at ``retrieve_output_path`` with."""
        return _ResultReader(self.serializer)

    def docker_script(self) -> str:
        """Return the shell script the container runs: check pixi, write the inputs, run the callable, write the result.

        The result file holds the result or, if the callable raised, the exit code of ``pixi run`` and the description
        of the exception, after which the container exits with 0, so that ``execute`` can raise
        [`PixiCallableError`][airflow.providers.pixi.exceptions.PixiCallableError] with it. Other failures exit with
        the code of ``pixi run``.
        """
        result = shlex.quote(self.result_path)
        error = f'"$work/output{ERROR_SUFFIX}"'
        return self.container_script(
            [
                f'if [ -f {error} ]; then {{ echo {_ERROR.decode()} "$status"; cat {error}; }} > {result}; exit 0; fi',
                '[ "$status" -eq 0 ] || exit "$status"',
                self.missing_result_check(),
                f'{{ echo {_RESULT.decode()}; cat "$work/output"; }} > {result}',
            ]
        )

    def execute(self, context: Any) -> Any:
        if self.env_vars is not None and not isinstance(self.env_vars, Mapping):
            # such as the string a templated env_vars renders to without render_template_as_native_obj
            raise TypeError(f"env_vars must be a dict of names and values, not {type(self.env_vars).__name__}")
        saved = self.environment, self._private_environment, self.entrypoint, self.command
        script = self.docker_script()
        self._private_environment = {**(self._private_environment or {}), **self.container_env(context)}
        self.entrypoint, self.command = ["sh", "-c", script], None
        # DockerOperator reads the container's variables from environment, which is the Pixi environment here
        self.environment = dict(self.env_vars or {})
        try:
            result = super().execute(context)
        finally:
            self.environment, self._private_environment, self.entrypoint, self.command = saved
        if isinstance(result, _CallableFailed):
            if result.exit_code in self.skip_on_exit_code:
                raise AirflowSkipException(f"pixi run exited with code {result.exit_code}, which skips the task")
            raise self._callable_error(result.error)
        if not isinstance(result, _Result):
            # DockerOperator returns None when it cannot copy the file out of the container
            raise AirflowException(
                f"The container finished, but its result could not be copied out of it from {self.result_path}"
            )
        return result.value
