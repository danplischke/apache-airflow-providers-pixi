"""Run a Python callable inside a Pixi environment, and the base classes of every Pixi operator."""

from __future__ import annotations

import collections
import contextlib
import hashlib
import inspect
import json
import logging
import os
import pickle
import shutil
import signal
import subprocess
import sys
import tempfile
from collections.abc import Callable, Container, Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, ClassVar, Literal

from packaging.utils import canonicalize_name

from airflow.providers.pixi.exceptions import PixiCallableError
from airflow.providers.pixi.hooks.pixi import pixi_auth_env
from airflow.providers.pixi.runtime.runner import ERROR_SUFFIX, PICKLE_PROTOCOL
from airflow.providers.pixi.utils.compat import AirflowException, AirflowSkipException, BranchMixIn, SkipMixin
from airflow.providers.pixi.utils.context import serializable_context
from airflow.providers.pixi.utils.env import check_env_sources, resolve_env
from airflow.providers.pixi.utils.manifest import build_pixi_toml, conda_dependencies, pypi_dependencies_table
from airflow.providers.pixi.utils.pixi import MIN_PIXI_VERSION, local_platform, resolve_pixi
from airflow.providers.pixi.utils.source import (
    RUNNER_SCRIPT,
    STRIPPED_DECORATORS,
    function_source,
    validate_callable,
)
from airflow.sdk import BaseOperator

__all__ = (
    "MIN_PIXI_VERSION",
    "WORKER_PYTHON_VARIABLES",
    "BasePixiOperator",
    "BasePixiPythonOperator",
    "PixiBranchOperator",
    "PixiOperator",
    "PixiRunEnvMixin",
    "PixiShortCircuitOperator",
    "PixiSubprocessMixin",
)

DEFAULT_CHANNELS = ("conda-forge",)
LOCK_MODES = ("locked", "frozen")

WORKER_PYTHON_VARIABLES = ("PYTHONPATH", "PYTHONHOME", "PYTHONUSERBASE", "VIRTUAL_ENV")
"""Variables of the worker's Python left out of a pixi run, so the environment's Python never imports the
worker's packages; ``env_vars`` or ``env`` can set them for a run."""

_JINJA_MARKERS = ("{{", "{%", "{#")

_DECORATOR_FUNCTIONS = (
    "pixi_task",
    "pixi_kubernetes_task",
    "pixi_sensor_task",
    "pixi_branch_task",
    "pixi_short_circuit_task",
)


def _needs_rendering(value: Any) -> bool:
    if isinstance(value, str):
        return any(marker in value for marker in _JINJA_MARKERS)
    if isinstance(value, dict):
        return any(_needs_rendering(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_needs_rendering(item) for item in value)
    return False


def _write_atomic(path: str, text: str) -> None:
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".pixi-airflow-")
    with os.fdopen(fd, "w") as f:
        f.write(text)
    os.replace(tmp, path)


def _signal_group(pgid: int, sig: signal.Signals) -> None:
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        pass
    except PermissionError:
        pass


def _terminate(proc: subprocess.Popen[bytes]) -> None:
    """Stop pixi and everything it started; it runs as the leader of its own process group."""
    if proc.poll() is None:
        _signal_group(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
    _signal_group(proc.pid, signal.SIGKILL)
    proc.wait()


class BasePixiOperator(BaseOperator):
    """Base class of the Pixi operators: chooses the Pixi environment and prepares it.

    The environment comes from exactly one of ``pixi_project_path`` (a project directory),
    ``pixi_toml_path`` (a ``pixi.toml`` or ``pyproject.toml``) or an inline manifest: a list of packages in
    ``dependencies`` / ``pypi_dependencies``, with ``channels`` and ``platforms``. Anything
    else, such as PyPI indexes, features or several environments, belongs in a ``pixi.toml``.

    :param pixi_project_path: a Pixi project directory, with a ``pixi.toml`` or ``pyproject.toml``
        (templated).
    :param pixi_toml_path: a ``pixi.toml`` or ``pyproject.toml``; pixi uses exactly that file (templated).
    :param environment: the environment of a project or manifest file to run in, if it defines several
        (templated). Not for inline manifests, which have only the default environment.
    :param lock_mode: how pixi treats the ``pixi.lock`` of a project or manifest file (templated). ``None``
        (default) runs plain ``pixi run``, which solves again and rewrites the lock file when it no longer
        matches the manifest. ``"locked"`` passes ``--locked``: the run fails if the lock file is out of date.
        ``"frozen"`` passes ``--frozen``: the environment is installed from the lock file as it is, without
        checking it against the manifest. Not for inline manifests, which have no lock file.
    :param dependencies: conda dependencies of an inline manifest, a dict or a list of MatchSpecs.
    :param pypi_dependencies: PyPI dependencies of an inline manifest (templated): a dict as under
        ``[pypi-dependencies]`` in a ``pixi.toml``, or pip requirement strings as ``@task.virtualenv`` takes them,
        a list or one string that may hold several lines, such as a rendered requirements file. An inline
        manifest with PyPI packages but no ``python`` dependency gets the worker's Python version.
    :param channels: channels of an inline manifest; default ``["conda-forge"]``.
    :param platforms: platforms of an inline manifest; default [`default_platforms`][default_platforms], the platform of
        the machine that runs pixi.
    :param env_cache_path: keep inline environments in ``<env_cache_path>/pixi-<hash>`` and reuse them across
        runs, like ``venv_cache_path`` of ``PythonVirtualenvOperator``. Otherwise an inline environment is
        built in a temporary directory, removed after the run unless ``cleanup_temp_manifest=False``.
    :param cleanup_temp_manifest: remove the temporary directory of an inline environment after the run.
    :param pixi_binary: the pixi executable, a name on ``PATH`` or a path. It must be installed on the workers, in
        version [`MIN_PIXI_VERSION`][airflow.providers.pixi.utils.pixi.MIN_PIXI_VERSION] or newer; the provider never
        installs it.

    Where pixi runs on the worker ([`local_manifest`][local_manifest]), a relative ``pixi_project_path``,
    ``pixi_toml_path`` or ``env_cache_path`` is relative to the directory of the DAG file there, or to the working
    directory for a task without a DAG.
    """

    template_fields: Sequence[str] = (
        "pixi_project_path",
        "pixi_toml_path",
        "environment",
        "lock_mode",
        "pypi_dependencies",
    )

    def __init__(
        self,
        *,
        pixi_project_path: str | None = None,
        pixi_toml_path: str | None = None,
        environment: str | None = None,
        lock_mode: Literal["locked", "frozen"] | None = None,
        dependencies: dict[str, Any] | Sequence[str] | None = None,
        pypi_dependencies: dict[str, Any] | Sequence[str] | str | None = None,
        channels: Sequence[str] | None = None,
        platforms: Sequence[str] | None = None,
        env_cache_path: str | None = None,
        cleanup_temp_manifest: bool = True,
        pixi_binary: str = "pixi",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.pixi_project_path = pixi_project_path
        self.pixi_toml_path = pixi_toml_path
        self.environment = environment
        self.lock_mode = lock_mode
        self.dependencies = dependencies
        self.pypi_dependencies = pypi_dependencies
        self.channels = channels
        self.platforms = platforms
        self.env_cache_path = env_cache_path
        self.cleanup_temp_manifest = cleanup_temp_manifest
        self.pixi_binary = pixi_binary
        self._held_manifest: tuple[str, str] | None = None

        has_inline = bool(dependencies) or bool(pypi_dependencies)
        if sum([pixi_project_path is not None, pixi_toml_path is not None, has_inline]) != 1:
            raise ValueError(
                "Exactly one of pixi_project_path, pixi_toml_path, or inline "
                "(dependencies / pypi_dependencies) must be provided."
            )
        if not _needs_rendering(pypi_dependencies):
            pypi_dependencies_table(pypi_dependencies)
        conda_dependencies(dependencies)
        if not _needs_rendering(environment):
            self._checked_environment(ValueError)
        if not _needs_rendering(lock_mode):
            self._checked_lock_mode(ValueError)

    @property
    def inline_manifest(self) -> bool:
        """Whether the environment comes from an inline manifest.

        Only an inline manifest can be extended with [`add_pypi_dependencies`][add_pypi_dependencies].
        """
        return self.pixi_project_path is None and self.pixi_toml_path is None

    def add_pypi_dependencies(self, *requirements: str) -> None:
        """Add pip requirement strings, such as ``"mytracker>=1"``, to the PyPI dependencies of the inline manifest.

        For subclasses and mixins that need a package in the environment, whichever form the DAG gave
        ``pypi_dependencies`` in: a dict gets the ``[pypi-dependencies]`` entries of ``requirements``, pip
        strings become a list with ``requirements`` at the end. ``pypi_dependencies`` is assigned a new value,
        so a value saved before stays as it was and can be restored afterwards.

        :raises AirflowException: if the environment comes from ``pixi_project_path`` or ``pixi_toml_path``,
            whose manifest has to list the packages itself.
        :raises ValueError: for a requirement ``pypi_dependencies`` does not accept, or a package listed twice.
        """
        if not self.inline_manifest:
            raise AirflowException(
                self._project_pypi_error(list(requirements), self.pixi_project_path or self.pixi_toml_path)
            )
        added = pypi_dependencies_table(list(requirements))
        current = self.pypi_dependencies
        if isinstance(current, dict):
            listed = {canonicalize_name(name) for name in current}
            for name in added:
                if canonicalize_name(name) in listed:
                    raise ValueError(f"{name} is listed twice in pypi_dependencies")
            self.pypi_dependencies = {**current, **added}
            return
        extended = [*([current] if isinstance(current, str) else current or []), *requirements]
        if not _needs_rendering(extended):
            pypi_dependencies_table(extended)
        self.pypi_dependencies = extended

    def inline_manifest_toml(self) -> str:
        """Return the ``pixi.toml`` of an inline manifest, from the rendered arguments."""
        pypi_dependencies = pypi_dependencies_table(self.pypi_dependencies)
        dependencies = conda_dependencies(self.dependencies)
        if pypi_dependencies and "python" not in dependencies:
            dependencies["python"] = f"{sys.version_info.major}.{sys.version_info.minor}.*"
        return build_pixi_toml(
            channels=self.channels or DEFAULT_CHANNELS,
            platforms=self.platforms or self.default_platforms(),
            dependencies=dependencies,
            pypi_dependencies=pypi_dependencies,
        )

    def default_platforms(self) -> list[str]:
        """Return the platforms of an inline manifest without ``platforms``: the one of the machine running pixi.

        Called when the task runs. Pixi solves the environment for every platform of the manifest, and a package
        missing on one of them fails the solve, so only the platform that runs it is listed.
        """
        return [local_platform()]

    def _checked_environment(self, error: type[Exception]) -> str | None:
        environment = self.environment or None
        if environment and self.inline_manifest:
            raise error(
                f"environment={environment!r} needs a manifest that defines it, and an inline manifest has only the "
                "default environment; define the environment in a pixi.toml and use pixi_project_path or "
                "pixi_toml_path"
            )
        return environment

    def _checked_lock_mode(self, error: type[Exception]) -> str | None:
        lock_mode = self.lock_mode or None
        if lock_mode not in (None, *LOCK_MODES):
            raise error(f"lock_mode must be one of {', '.join(map(repr, LOCK_MODES))} or None, not {lock_mode!r}")
        if lock_mode and self.inline_manifest:
            raise error(
                f"lock_mode={lock_mode!r} needs a pixi.lock, which an inline manifest does not have; "
                "use pixi_project_path or pixi_toml_path"
            )
        return lock_mode

    def pixi_run_options(self) -> list[str]:
        """Return the options of ``pixi run`` that follow ``--manifest-path``: the environment and the lock mode.

        Called when the task runs, as templated fields are then rendered.
        """
        environment = self._checked_environment(AirflowException)
        options = ["--environment", environment] if environment else []
        lock_mode = self._checked_lock_mode(AirflowException)
        if lock_mode:
            options.append(f"--{lock_mode}")
        return options

    def pixi_run_command(self, pixi: str, manifest: str) -> list[str]:
        """Return ``pixi run`` with the manifest and [`pixi_run_options`][pixi_run_options].

        The command to run in the environment goes after it.
        """
        return [pixi, "run", "--manifest-path", manifest, *self.pixi_run_options()]

    def _local_path(self, path: str) -> str:
        if not os.path.isabs(path) and (dag := self.get_dag()) is not None and dag.fileloc:
            path = os.path.join(os.path.dirname(dag.fileloc), path)
        return os.path.abspath(path)

    @staticmethod
    def _project_pypi_error(pypi_dependencies: Any, manifest: str | None) -> str:
        return (
            f"pypi_dependencies {pypi_dependencies!r} can only extend an inline manifest; "
            f"add them to the environment of {manifest} instead"
        )

    def _check_project_manifest(self, manifest: str) -> None:
        if self.pypi_dependencies:
            raise AirflowException(self._project_pypi_error(self.pypi_dependencies, manifest))

    @contextlib.contextmanager
    def local_manifest(self) -> Iterator[tuple[str, str]]:
        """Yield the manifest to pass to pixi on the worker and the directory to run in.

        An inline manifest is written to ``env_cache_path`` or to a temporary directory, removed afterwards
        unless ``cleanup_temp_manifest=False``. Runs at execution time, so templated paths are rendered. Relative
        paths are resolved against the directory of the DAG file.
        """
        if self._held_manifest is not None:
            yield self._held_manifest
            return
        if self.pixi_project_path is not None:
            project = self._local_path(self.pixi_project_path)
            self._check_project_manifest(project)
            yield project, project
            return
        if self.pixi_toml_path is not None:
            manifest = self._local_path(self.pixi_toml_path)
            self._check_project_manifest(manifest)
            yield manifest, os.path.dirname(manifest)
            return
        toml = self.inline_manifest_toml()
        if self.env_cache_path:
            digest = hashlib.sha256(toml.encode()).hexdigest()[:16]
            env_dir = os.path.join(self._local_path(self.env_cache_path), f"pixi-{digest}")
            os.makedirs(env_dir, exist_ok=True)
            manifest = os.path.join(env_dir, "pixi.toml")
            if not os.path.exists(manifest):
                _write_atomic(manifest, toml)
            yield manifest, env_dir
            return
        env_dir = tempfile.mkdtemp(prefix="airflow_pixi_")
        try:
            manifest = os.path.join(env_dir, "pixi.toml")
            Path(manifest).write_text(toml)
            yield manifest, env_dir
        finally:
            if self.cleanup_temp_manifest:
                shutil.rmtree(env_dir, ignore_errors=True)

    @contextlib.contextmanager
    def holding_manifest(self) -> Iterator[None]:
        """Keep one [`local_manifest`][local_manifest] for every use inside, such as the pokes of one sensor run."""
        with self.local_manifest() as held:
            self._held_manifest = held
            try:
                yield
            finally:
                self._held_manifest = None


class BasePixiPythonOperator(BasePixiOperator):
    """Base class of the Pixi operators that call a Python function inside the environment.

    Accepts every [`BasePixiOperator`][BasePixiOperator] argument, plus:

    :param python_callable: a function, whose source is shipped to the environment and run there, so it
        must be self-contained (imports inside it, no variables from enclosing functions); or a
        ``"module.path:callable_name"`` string imported in the environment. The manifest's directory is the
        working directory, so modules next to it are importable.
    :param op_args: positional arguments for the callable (templated).
    :param op_kwargs: keyword arguments for the callable (templated).
    :param serializer: how arguments and the return value travel: ``"json"`` (default), or ``"pickle"`` for
        values JSON cannot hold, whose types must then be importable on both sides.

    Parameters of the callable named after a key of
    [`CONTEXT_KEYS`][airflow.providers.pixi.utils.context.CONTEXT_KEYS], such as ``ds``, ``params`` or ``run_id``, get
    the task's value when ``op_args`` and ``op_kwargs`` leave them unset, and a ``**kwargs`` parameter gets all
    of them, as for ``@task.virtualenv``. Dates and datetimes arrive as ISO 8601 strings.

    Subclasses and mixins can change what runs, the same way as for ``@task.virtualenv``: override
    [`get_python_source`][get_python_source], and set ``op_args`` or ``op_kwargs``, or call
    [`add_pypi_dependencies`][BasePixiOperator.add_pypi_dependencies], in ``execute`` before calling
    ``super().execute``.
    """

    template_fields: Sequence[str] = (*BasePixiOperator.template_fields, "op_args", "op_kwargs")
    template_fields_renderers: ClassVar[dict[str, str]] = {"op_args": "py", "op_kwargs": "py"}
    shallow_copy_attrs: Sequence[str] = ("python_callable",)

    def __init__(
        self,
        *,
        python_callable: Callable[..., Any] | str,
        op_args: Sequence[Any] | None = None,
        op_kwargs: dict[str, Any] | None = None,
        serializer: Literal["json", "pickle"] = "json",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.serializer = serializer
        if serializer not in ("json", "pickle"):
            raise ValueError(f"serializer must be 'json' or 'pickle', not {serializer!r}")
        validate_callable(python_callable)
        if not isinstance(python_callable, str) and (free := inspect.unwrap(python_callable).__code__.co_freevars):
            raise ValueError(
                f"{python_callable.__name__} uses {', '.join(free)} from an enclosing function, which does not "
                "exist in the Pixi environment; pass it as an argument instead"
            )

    def get_python_source(self) -> str:
        """Return the source shipped to the environment when ``python_callable`` is a function.

        The runner executes it and calls the name of ``python_callable`` in the resulting namespace. Task
        decorators are removed, including the one named by ``custom_operator_name`` (``@task.<name>`` of a
        decorator built on ``@task.pixi``) and this provider's decorator functions used under their own names
        (``@pixi_task(...)``), and line numbers kept, so tracebacks point at the DAG file. Override it to wrap
        the function, for example by appending code that rebinds the name to a wrapper, as for
        ``@task.virtualenv``.
        """
        stripped = {*STRIPPED_DECORATORS, *_DECORATOR_FUNCTIONS}
        operator_name = getattr(self, "custom_operator_name", "")
        if operator_name.startswith("@"):
            stripped.add(operator_name[1:])
        return function_source(self.python_callable, stripped)[0]  # type: ignore[arg-type]

    def callable_input(self, context: Mapping[str, Any] | None = None) -> bytes:
        """Return the runner's input: the callable, its arguments and the task context, in the ``serializer``'s format.

        :param context: the task context, of which the runner passes the JSON-safe part
            ([`serializable_context`][airflow.providers.pixi.utils.context.serializable_context]) to parameters named
            after its keys.
        """
        if isinstance(self.python_callable, str):
            module, _, name = self.python_callable.partition(":")
            spec: dict[str, Any] = {"module": module, "name": name}
        else:
            fn = inspect.unwrap(self.python_callable)
            spec = {
                "source": self.get_python_source(),
                "filename": inspect.getsourcefile(fn) or "<pixi task>",
                "name": fn.__name__,
            }
        spec.update(args=list(self.op_args), kwargs=dict(self.op_kwargs))
        if context_values := serializable_context(context):
            spec["context"] = context_values
        if self.serializer == "pickle":
            return pickle.dumps(spec, protocol=PICKLE_PROTOCOL)
        try:
            return json.dumps(spec).encode()
        except TypeError as e:
            raise TypeError(f"op_args and op_kwargs must be JSON-serializable, or pass serializer='pickle': {e}") from e

    def _callable_name(self) -> str:
        return getattr(self.python_callable, "__name__", self.python_callable)  # type: ignore[return-value]

    def _missing_result(self) -> AirflowException:
        return AirflowException(f"{self._callable_name()} exited without returning, for example through sys.exit()")

    def _callable_error(self, error: dict[str, str]) -> PixiCallableError:
        """Return the exception for the runner's error file, ``{"type", "message", "traceback"}``."""
        return PixiCallableError(self._callable_name(), error["type"], error["message"], error["traceback"])


class PixiRunEnvMixin(BaseOperator):
    """The environment variables of a pixi run on the worker, from Airflow Variables and Connections.

    Everything is read when the task runs, never rendered as a template.

    :param env_from_variables: environment variable name to the key of an Airflow Variable, such as
        ``{"API_URL": "api_url"}``. A missing Variable fails the task.
    :param env_from_connections: environment variable name to ``"conn_id"`` (the connection's URI),
        ``"conn_id.<field>"`` (``host``, ``login``, ``password``, ``schema``, ``port``, or ``extra`` as a JSON
        string) or ``"conn_id.extra.<key>"`` (one key of the extra), such as
        ``{"DB_PASSWORD": "warehouse.password"}``. A missing connection or field fails the task. Passwords, URIs
        and extras are masked in the task log.
    :param pixi_conn_id: one or several connections of type ``pixi``
        ([`PixiHook`][airflow.providers.pixi.hooks.pixi.PixiHook]) with credentials for private conda channels or
        PyPI indexes. Written to temporary files for the run, which ``RATTLER_AUTH_FILE`` and ``NETRC`` point at.
    :param pixi_cache_dir_variable: key of an Airflow Variable holding ``PIXI_CACHE_DIR`` for the run (templated);
        likewise ``uv_cache_dir_variable`` (``UV_CACHE_DIR``) and ``pip_cache_dir_variable`` (``PIP_CACHE_DIR``).
        A missing Variable leaves the environment variable unset.

    [`pixi_run_env`][pixi_run_env] builds the run's environment from them.
    """

    template_fields: Sequence[str] = ("pixi_cache_dir_variable", "uv_cache_dir_variable", "pip_cache_dir_variable")

    def __init__(
        self,
        *,
        env_from_variables: dict[str, str] | None = None,
        env_from_connections: dict[str, str] | None = None,
        pixi_conn_id: str | Sequence[str] | None = None,
        pixi_cache_dir_variable: str | None = None,
        uv_cache_dir_variable: str | None = None,
        pip_cache_dir_variable: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        check_env_sources(env_from_variables, env_from_connections)
        self.env_from_variables = env_from_variables
        self.env_from_connections = env_from_connections
        self.pixi_conn_id = pixi_conn_id
        self.pixi_cache_dir_variable = pixi_cache_dir_variable
        self.uv_cache_dir_variable = uv_cache_dir_variable
        self.pip_cache_dir_variable = pip_cache_dir_variable

    @contextlib.contextmanager
    def pixi_run_env(
        self, base: Mapping[str, str], overrides: Mapping[str, str] | None = None
    ) -> Iterator[dict[str, str]]:
        """Yield the environment of a pixi run, valid until the block exits.

        In increasing precedence: ``base`` (usually the worker's environment) without the worker's Python settings
        ([`WORKER_PYTHON_VARIABLES`][WORKER_PYTHON_VARIABLES]) and with ``PYTHONNOUSERSITE=1``, so the environment's
        Python imports only what the Pixi environment has; the cache directory Variables; ``env_from_variables`` and
        ``env_from_connections``; then ``overrides`` (such as ``env_vars``), which can set those Python variables again.
        With ``pixi_conn_id``, ``RATTLER_AUTH_FILE`` and ``NETRC`` point at temporary files with the credentials, merged
        with the files these variables named before (see
        [`pixi_auth_env`][airflow.providers.pixi.hooks.pixi.pixi_auth_env]), and removed when the block exits.
        """
        env = {name: value for name, value in base.items() if name not in WORKER_PYTHON_VARIABLES}
        env["PYTHONNOUSERSITE"] = "1"
        env.update(
            resolve_env(
                optional_variables={
                    "PIXI_CACHE_DIR": self.pixi_cache_dir_variable,
                    "UV_CACHE_DIR": self.uv_cache_dir_variable,
                    "PIP_CACHE_DIR": self.pip_cache_dir_variable,
                },
                env_from_variables=self.env_from_variables,
                env_from_connections=self.env_from_connections,
            )
        )
        env.update(overrides or {})
        with pixi_auth_env(self.pixi_conn_id, env) as auth:
            env.update(auth)
            yield env


class PixiSubprocessMixin(PixiRunEnvMixin):
    """Run the runner with pixi on the worker, streaming its output to the task log.

    Accepts every [`PixiRunEnvMixin`][PixiRunEnvMixin] argument, plus:

    :param env_vars: environment variables for the run, on top of all others. Not templated, so secrets
        set here are never rendered into the UI.
    :param skip_on_exit_code: exit codes of ``pixi run`` that skip the task instead of failing it, as for
        ``PythonVirtualenvOperator``. The callable chooses one with ``sys.exit(code)``; an exception it raises
        exits with 1, and so does pixi when it cannot prepare the environment.

    An exception raised by the callable fails the task with
    [`PixiCallableError`][airflow.providers.pixi.exceptions.PixiCallableError], which names the callable and the
    exception; other failures of the run fail it with pixi's exit code and the last lines of output.
    ``execution_timeout`` and killing the task stop pixi and everything it started.
    """

    template_fields: Sequence[str] = PixiRunEnvMixin.template_fields

    def __init__(
        self,
        *,
        env_vars: dict[str, str] | None = None,
        skip_on_exit_code: int | Container[int] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.env_vars = env_vars
        self.skip_on_exit_code: Container[int] = (
            skip_on_exit_code
            if isinstance(skip_on_exit_code, Container)
            else [skip_on_exit_code]
            if skip_on_exit_code is not None
            else []
        )
        self._process: subprocess.Popen[bytes] | None = None

    def run_callable(self: Any, context: Mapping[str, Any] | None = None) -> Any:
        """Call ``python_callable`` in the environment and return its result.

        :param context: the task context, for the callable's parameters named after its keys.
        """
        pixi = resolve_pixi(self.pixi_binary)
        with (
            self.local_manifest() as (manifest, cwd),
            tempfile.TemporaryDirectory(prefix="airflow_pixi_io_") as io,
            self.pixi_run_env(os.environ, self.env_vars) as env,
        ):
            env.setdefault("PYTHONUNBUFFERED", "1")  # the callable's output reaches the log as it prints
            input_path, output_path = os.path.join(io, "input"), os.path.join(io, "output")
            Path(input_path).write_bytes(self.callable_input(context))
            command = self.pixi_run_command(pixi, manifest)
            self.log.info("Running %s in %s", " ".join(command), cwd)
            returncode, tail = self._run(
                [*command, "python", "-c", RUNNER_SCRIPT, self.serializer, input_path, output_path], cwd, env
            )
            if returncode in self.skip_on_exit_code:
                raise AirflowSkipException(f"pixi run exited with code {returncode}, which skips the task")
            if returncode != 0:
                error_path = Path(output_path + ERROR_SUFFIX)
                if error_path.exists():
                    raise self._callable_error(json.loads(error_path.read_text()))
                raise AirflowException(f"pixi run exited with code {returncode}:\n" + "\n".join(tail))
            if not os.path.exists(output_path):
                raise self._missing_result()
            if self.serializer == "pickle":
                return pickle.loads(Path(output_path).read_bytes())
            return json.loads(Path(output_path).read_text())

    def on_kill(self) -> None:
        if self._process is not None:
            _terminate(self._process)

    def _run(self, cmd: list[str], cwd: str, env: Mapping[str, str]) -> tuple[int, list[str]]:
        """Run ``cmd``, streaming its output to the log; return its exit code and its last lines of output."""
        tail: collections.deque[str] = collections.deque(maxlen=20)
        with subprocess.Popen(
            cmd,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,  # so the whole tree can be stopped, see _terminate
        ) as proc:
            self._process = proc
            try:
                for raw in iter(proc.stdout.readline, b""):  # type: ignore[union-attr]
                    line = raw.decode(errors="backslashreplace").rstrip()
                    tail.append(line)
                    self.log.info("%s", line)
                returncode = proc.wait()
            except BaseException:
                # execution_timeout raises here; stop the run before the task fails
                _terminate(proc)
                raise
            finally:
                self._process = None
        return returncode, list(tail)


class PixiOperator(PixiSubprocessMixin, BasePixiPythonOperator):
    """Run a Python callable inside a Pixi environment on the worker.

    Accepts every [`BasePixiPythonOperator`][BasePixiPythonOperator] and [`BasePixiOperator`][BasePixiOperator]
    argument, plus those of [`PixiSubprocessMixin`][PixiSubprocessMixin] and [`PixiRunEnvMixin`][PixiRunEnvMixin]:
    ``env_vars``, ``env_from_variables``, ``env_from_connections``, ``pixi_conn_id``, the cache directory Variables and
    ``skip_on_exit_code``. The return value is the task's XCom. The output of the run is streamed to the task log;
    ``execution_timeout`` and killing the task stop pixi and everything it started.

    Subclasses and mixins can change what runs, the same way as for ``@task.virtualenv``: override
    [`get_python_source`][BasePixiPythonOperator.get_python_source], and set ``op_args``, ``op_kwargs`` or ``env_vars``,
    or call [`add_pypi_dependencies`][BasePixiOperator.add_pypi_dependencies], in ``execute`` before calling
    ``super().execute``.
    """

    template_fields: Sequence[str] = (*BasePixiPythonOperator.template_fields, *PixiSubprocessMixin.template_fields)
    custom_operator_name = "Pixi"

    def execute(self, context: Any) -> Any:
        return self.run_callable(context)


class PixiBranchOperator(PixiOperator, BranchMixIn):
    """Choose the tasks to follow with a Python callable run inside a Pixi environment on the worker.

    The Pixi counterpart of ``BranchPythonVirtualenvOperator``. The callable returns a task id or task group id
    directly downstream of this task, a list of them, or ``None``. The tasks directly downstream that it does not
    name are skipped, and the skips spread to the tasks after them as their trigger rules decide; ``None`` skips
    them all. A task group id stands for the group's root tasks. As for ``BranchPythonOperator``, the return value
    becomes the task's XCom only when nothing is skipped: otherwise Airflow's task runner handles the skips instead.

    Accepts every [`PixiOperator`][PixiOperator] argument; context parameters such as ``params`` reach the callable as
    for [`PixiOperator`][PixiOperator]. A task id that is not in the DAG fails the task, and so does a return value that
    is neither a string, a list of strings nor ``None``.
    """

    inherits_from_skipmixin = True
    custom_operator_name = "PixiBranch"

    def execute(self, context: Any) -> Any:
        return self.do_branch(context, super().execute(context))


class PixiShortCircuitOperator(PixiOperator, SkipMixin):
    """Continue the pipeline only if a Python callable run inside a Pixi environment returns a truthy value.

    The Pixi counterpart of ``ShortCircuitOperator``. When the callable returns a falsy value, the tasks
    downstream of this one are skipped, except teardown tasks. As for ``ShortCircuitOperator``, the return value
    becomes the task's XCom only when nothing is skipped: a truthy one, or a falsy one without downstream tasks.

    Accepts every [`PixiOperator`][PixiOperator] argument; context parameters such as ``params`` reach the callable as
    for [`PixiOperator`][PixiOperator]. Plus:

    :param ignore_downstream_trigger_rules: if ``True`` (default), skip every task downstream of this one,
        whatever its trigger rule. If ``False``, skip only the tasks directly downstream, and let the trigger
        rules of the tasks after them decide whether they run.
    """

    inherits_from_skipmixin = True
    custom_operator_name = "PixiShortCircuit"

    def __init__(self, *, ignore_downstream_trigger_rules: bool = True, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.ignore_downstream_trigger_rules = ignore_downstream_trigger_rules

    def execute(self, context: Any) -> Any:
        condition = super().execute(context)
        self.log.info("Condition result is %s", condition)

        if condition:
            self.log.info("Proceeding with downstream tasks...")
            return condition

        if not self.downstream_task_ids:
            self.log.info("No downstream tasks; nothing to do.")
            return condition

        def get_tasks_to_skip() -> Iterator[Any]:
            if self.ignore_downstream_trigger_rules is True:
                tasks = context["task"].get_flat_relatives(upstream=False)
            else:
                tasks = context["task"].get_direct_relatives(upstream=False)
            for t in tasks:
                if not t.is_teardown:
                    yield t

        to_skip: Iterable[Any] = get_tasks_to_skip()
        if self.log.getEffectiveLevel() <= logging.DEBUG:
            self.log.debug("Downstream task IDs %s", to_skip := list(get_tasks_to_skip()))

        self.log.info("Skipping downstream tasks")
        self.skip(ti=context["ti"], tasks=to_skip)
        self.log.info("Done.")
        return condition
