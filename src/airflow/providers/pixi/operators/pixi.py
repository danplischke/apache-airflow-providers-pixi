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

from airflow.sdk import BaseOperator
from packaging.utils import canonicalize_name

from airflow.providers.pixi.exceptions import PixiCallableError
from airflow.providers.pixi.hooks.pixi import pixi_auth_env
from airflow.providers.pixi.utils.compat import AirflowException, AirflowSkipException, BranchMixIn, SkipMixin
from airflow.providers.pixi.utils.context import serializable_context
from airflow.providers.pixi.utils.env import check_env_sources, resolve_env
from airflow.providers.pixi.utils.manifest import (
    build_pixi_toml,
    check_manifest,
    conda_dependencies,
    manifest_table,
    pypi_dependencies_from_requirements,
)
from airflow.providers.pixi.utils.pixi import MIN_PIXI_VERSION, local_platform, resolve_pixi
from airflow.providers.pixi.utils.source import (
    ERROR_SUFFIX,
    PICKLE_PROTOCOL,
    RUNNER_SCRIPT,
    STRIPPED_DECORATORS,
    function_source,
    validate_callable,
)

__all__ = [
    "MIN_PIXI_VERSION",
    "BasePixiOperator",
    "BasePixiPythonOperator",
    "PixiBranchOperator",
    "PixiOperator",
    "PixiRunEnvMixin",
    "PixiShortCircuitOperator",
    "PixiSubprocessMixin",
]

DEFAULT_CHANNELS = ("conda-forge",)
LOCK_MODES = ("locked", "frozen")

_DECORATOR_FUNCTIONS = (
    "pixi_task",
    "pixi_kubernetes_task",
    "pixi_sensor_task",
    "pixi_branch_task",
    "pixi_short_circuit_task",
)


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
    ``pixi_toml_path`` (a ``pixi.toml`` or ``pyproject.toml``) or an inline manifest:
    ``dependencies`` / ``pypi_dependencies`` / ``requirements`` plus ``channels``, ``platforms``,
    ``workspace_name``, ``pypi_options``, ``environments`` and ``feature``, as in ``pixi.toml``.

    :param pixi_project_path: a Pixi project directory, with a ``pixi.toml`` or ``pyproject.toml``
        (templated).
    :param pixi_toml_path: a ``pixi.toml`` or ``pyproject.toml``; pixi uses exactly that file (templated).
    :param environment: the manifest environment to run in, if it defines several (templated).
    :param lock_mode: how pixi treats the ``pixi.lock`` of a project or manifest file (templated). ``None``
        (default) runs plain ``pixi run``, which solves again and rewrites the lock file when it no longer
        matches the manifest. ``"locked"`` passes ``--locked``: the run fails if the lock file is out of date.
        ``"frozen"`` passes ``--frozen``: the environment is installed from the lock file as it is, without
        checking it against the manifest. Not for inline manifests, which have no lock file.
    :param requirements: pip requirement strings, as ``@task.virtualenv`` takes them (templated), added to an
        inline manifest's ``[pypi-dependencies]``. An inline manifest with PyPI packages but no ``python``
        dependency gets the worker's Python version.
    :param dependencies: conda dependencies of an inline manifest, a dict or a list of MatchSpecs.
    :param pypi_dependencies: ``[pypi-dependencies]`` of an inline manifest.
    :param channels: channels of an inline manifest; default ``["conda-forge"]``.
    :param platforms: platforms of an inline manifest; default :meth:`default_platforms`, the platform of the
        machine that runs pixi.
    :param workspace_name: ``[workspace] name`` of an inline manifest.
    :param pypi_options: ``[pypi-options]`` of an inline manifest.
    :param environments: ``[environments]`` of an inline manifest.
    :param feature: ``[feature.<name>]`` tables of an inline manifest, with ``channels``, ``platforms``,
        ``dependencies`` and ``pypi_dependencies``.
    :param env_cache_path: keep inline environments in ``<env_cache_path>/pixi-<hash>`` and reuse them across
        runs, like ``venv_cache_path`` of ``PythonVirtualenvOperator``. Otherwise an inline environment is
        built in a temporary directory, removed after the run unless ``cleanup_temp_manifest=False``.
    :param cleanup_temp_manifest: remove the temporary directory of an inline environment after the run.
    :param validate_manifest: check an inline manifest against pixi's manifest schema for
        :data:`MIN_PIXI_VERSION`, when the DAG is parsed and again when the task runs. Turn it off for keys that
        only a newer pixi on the workers knows.
    :param pixi_binary: the pixi executable, a name on ``PATH`` or a path. It must be installed on the workers,
        in version :data:`MIN_PIXI_VERSION` or newer; the provider never installs it.

    Where pixi runs on the worker (:meth:`local_manifest`), a relative ``pixi_project_path``, ``pixi_toml_path``
    or ``env_cache_path`` is relative to the directory of the DAG file there, or to the working directory for a
    task without a DAG.
    """

    template_fields: Sequence[str] = (
        "pixi_project_path",
        "pixi_toml_path",
        "environment",
        "lock_mode",
        "requirements",
    )

    def __init__(
        self,
        *,
        pixi_project_path: str | None = None,
        pixi_toml_path: str | None = None,
        environment: str | None = None,
        lock_mode: Literal["locked", "frozen"] | None = None,
        requirements: Sequence[str] | str | None = None,
        dependencies: dict[str, Any] | Sequence[str] | None = None,
        pypi_dependencies: dict[str, Any] | None = None,
        channels: Sequence[str] | None = None,
        platforms: Sequence[str] | None = None,
        workspace_name: str | None = None,
        pypi_options: dict[str, Any] | None = None,
        environments: dict[str, Any] | None = None,
        feature: dict[str, Any] | None = None,
        env_cache_path: str | None = None,
        cleanup_temp_manifest: bool = True,
        validate_manifest: bool = True,
        pixi_binary: str = "pixi",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.pixi_project_path = pixi_project_path
        self.pixi_toml_path = pixi_toml_path
        self.environment = environment
        self.lock_mode = lock_mode
        self.requirements = [requirements] if isinstance(requirements, str) else list(requirements or [])
        self.dependencies = dependencies
        self.pypi_dependencies = pypi_dependencies
        self.channels = channels
        self.platforms = platforms
        self.workspace_name = workspace_name
        self.pypi_options = pypi_options
        self.environments = environments
        self.feature = feature
        self.env_cache_path = env_cache_path
        self.cleanup_temp_manifest = cleanup_temp_manifest
        self.validate_manifest = validate_manifest
        self.pixi_binary = pixi_binary
        self._held_manifest: tuple[str, str] | None = None

        has_inline = bool(dependencies) or bool(pypi_dependencies) or bool(self.requirements)
        if sum([pixi_project_path is not None, pixi_toml_path is not None, has_inline]) != 1:
            raise ValueError(
                "Exactly one of pixi_project_path, pixi_toml_path, or inline "
                "(dependencies / pypi_dependencies / requirements) must be provided."
            )
        if not any("{{" in r for r in self.requirements):
            pypi_dependencies_from_requirements(self.requirements)
        if validate_manifest and has_inline:
            check_manifest(
                manifest_table(
                    channels=channels or DEFAULT_CHANNELS,
                    platforms=platforms or ["linux-64"],
                    workspace_name=workspace_name,
                    dependencies=dependencies,
                    pypi_dependencies=pypi_dependencies,
                    pypi_options=pypi_options,
                    environments=environments,
                    feature=feature,
                )
            )
        if not (isinstance(lock_mode, str) and "{{" in lock_mode):
            self._checked_lock_mode(ValueError)

    @property
    def inline_manifest(self) -> bool:
        """Whether the environment comes from an inline manifest, the only one ``requirements`` can extend."""
        return self.pixi_project_path is None and self.pixi_toml_path is None

    def inline_manifest_toml(self) -> str:
        """Return the ``pixi.toml`` of an inline manifest, from the rendered arguments."""
        pypi_dependencies = dict(self.pypi_dependencies or {})
        listed = {canonicalize_name(name) for name in pypi_dependencies}
        for name, spec in pypi_dependencies_from_requirements(self.requirements).items():
            if canonicalize_name(name) in listed:
                raise ValueError(f"{name} is listed in both pypi_dependencies and requirements")
            pypi_dependencies[name] = spec
        dependencies = conda_dependencies(self.dependencies)
        if pypi_dependencies and "python" not in dependencies and not self.feature:
            dependencies["python"] = f"{sys.version_info.major}.{sys.version_info.minor}.*"
        return build_pixi_toml(
            channels=self.channels or DEFAULT_CHANNELS,
            platforms=self.platforms or self.default_platforms(),
            workspace_name=self.workspace_name,
            dependencies=dependencies,
            pypi_dependencies=pypi_dependencies,
            pypi_options=self.pypi_options,
            environments=self.environments,
            feature=self.feature,
            validate_manifest=self.validate_manifest,
        )

    def default_platforms(self) -> list[str]:
        """Return the platforms of an inline manifest without ``platforms``: the one of the machine running pixi.

        Called when the task runs. Pixi solves the environment for every platform of the manifest, and a package
        missing on one of them fails the solve, so only the platform that runs it is listed.
        """
        return [local_platform()]

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
        options = ["--environment", self.environment] if self.environment else []
        lock_mode = self._checked_lock_mode(AirflowException)
        if lock_mode:
            options.append(f"--{lock_mode}")
        return options

    def pixi_run_command(self, pixi: str, manifest: str) -> list[str]:
        """Return ``pixi run`` with the manifest and :meth:`pixi_run_options`, ready for the command to run."""
        return [pixi, "run", "--manifest-path", manifest, *self.pixi_run_options()]

    def _local_path(self, path: str) -> str:
        if not os.path.isabs(path) and (dag := self.get_dag()) is not None and dag.fileloc:
            path = os.path.join(os.path.dirname(dag.fileloc), path)
        return os.path.abspath(path)

    def _check_project_manifest(self, manifest: str) -> None:
        if self.requirements:
            raise AirflowException(
                f"requirements {self.requirements} can only extend an inline manifest; "
                f"add them to the environment of {manifest} instead"
            )

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
        """Keep one :meth:`local_manifest` for every use inside, such as the pokes of one sensor run."""
        with self.local_manifest() as held:
            self._held_manifest = held
            try:
                yield
            finally:
                self._held_manifest = None


class BasePixiPythonOperator(BasePixiOperator):
    """Base class of the Pixi operators that call a Python function inside the environment.

    Accepts every :class:`BasePixiOperator` argument, plus:

    :param python_callable: a function, whose source is shipped to the environment and run there, so it
        must be self-contained (imports inside it, no variables from enclosing functions); or a
        ``"module.path:callable_name"`` string imported in the environment. The manifest's directory is the
        working directory, so modules next to it are importable.
    :param op_args: positional arguments for the callable (templated).
    :param op_kwargs: keyword arguments for the callable (templated).
    :param serializer: how arguments and the return value travel: ``"json"`` (default), or ``"pickle"`` for
        values JSON cannot hold, whose types must then be importable on both sides.

    Parameters of the callable named after a key of
    :data:`~airflow.providers.pixi.utils.context.CONTEXT_KEYS`, such as ``ds``, ``params`` or ``run_id``, get
    the task's value when ``op_args`` and ``op_kwargs`` leave them unset, and a ``**kwargs`` parameter gets all
    of them, as for ``@task.virtualenv``. Dates and datetimes arrive as ISO 8601 strings.

    Subclasses and mixins can change what runs, the same way as for ``@task.virtualenv``: override
    :meth:`get_python_source`, and set ``op_args``, ``op_kwargs`` or ``requirements`` in ``execute`` before
    calling ``super().execute``.
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
            (:func:`~airflow.providers.pixi.utils.context.serializable_context`) to parameters named after its keys.
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
        (:class:`~airflow.providers.pixi.hooks.pixi.PixiHook`) with credentials for private conda channels or
        PyPI indexes. Written to temporary files for the run, which ``RATTLER_AUTH_FILE`` and ``NETRC`` point at.
    :param pixi_cache_dir_variable: key of an Airflow Variable holding ``PIXI_CACHE_DIR`` for the run (templated);
        likewise ``uv_cache_dir_variable`` (``UV_CACHE_DIR``) and ``pip_cache_dir_variable`` (``PIP_CACHE_DIR``).
        A missing Variable leaves the environment variable unset.

    :meth:`pixi_run_env` builds the run's environment from them.
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

        In increasing precedence: ``base`` (usually the worker's environment), the cache directory Variables,
        ``env_from_variables`` and ``env_from_connections``, then ``overrides`` (such as ``env_vars``). With
        ``pixi_conn_id``, ``RATTLER_AUTH_FILE`` and ``NETRC`` point at temporary files with the credentials,
        merged with the files these variables named before (see
        :func:`~airflow.providers.pixi.hooks.pixi.pixi_auth_env`), and removed when the block exits.
        """
        env = dict(base)
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

    Accepts every :class:`PixiRunEnvMixin` argument, plus:

    :param env_vars: environment variables for the run, on top of all others. Not templated, so secrets
        set here are never rendered into the UI.
    :param skip_on_exit_code: exit codes of ``pixi run`` that skip the task instead of failing it, as for
        ``PythonVirtualenvOperator``. The callable chooses one with ``sys.exit(code)``; an exception it raises
        exits with 1, and so does pixi when it cannot prepare the environment.

    An exception raised by the callable fails the task with
    :class:`~airflow.providers.pixi.exceptions.PixiCallableError`, which names the callable and the exception;
    other failures of the run fail it with pixi's exit code and the last lines of output.
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

    Accepts every :class:`BasePixiPythonOperator` and :class:`BasePixiOperator` argument, plus those of
    :class:`PixiSubprocessMixin` and :class:`PixiRunEnvMixin`: ``env_vars``, ``env_from_variables``,
    ``env_from_connections``, ``pixi_conn_id``, the cache directory Variables and ``skip_on_exit_code``. The
    return value is the task's XCom. The output of the run is streamed to the task log; ``execution_timeout``
    and killing the task stop pixi and everything it started.

    Subclasses and mixins can change what runs, the same way as for ``@task.virtualenv``: override
    :meth:`get_python_source`, and set ``op_args``, ``op_kwargs``, ``env_vars`` or ``requirements`` in
    ``execute`` before calling ``super().execute``.
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

    Accepts every :class:`PixiOperator` argument; context parameters such as ``params`` reach the callable as for
    :class:`PixiOperator`. A task id that is not in the DAG fails the task, and so does a return value that is
    neither a string, a list of strings nor ``None``.
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

    Accepts every :class:`PixiOperator` argument; context parameters such as ``params`` reach the callable as for
    :class:`PixiOperator`. Plus:

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
