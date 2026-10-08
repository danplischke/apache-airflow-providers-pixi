"""Run a Python callable inside a Pixi environment, and the base classes of every Pixi operator."""

from __future__ import annotations

import collections
import contextlib
import hashlib
import inspect
import json
import os
import pickle
import shutil
import signal
import subprocess
import sys
import tempfile
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from typing import Any, ClassVar, Literal

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator, Variable
from packaging.utils import canonicalize_name

from airflow.providers.pixi.utils.manifest import (
    build_pixi_toml,
    conda_dependencies,
    pypi_dependencies_from_requirements,
)
from airflow.providers.pixi.utils.pixi import MIN_PIXI_VERSION, resolve_pixi
from airflow.providers.pixi.utils.source import (
    PICKLE_PROTOCOL,
    RUNNER_SCRIPT,
    STRIPPED_DECORATORS,
    function_source,
    validate_callable,
)

__all__ = ["MIN_PIXI_VERSION", "BasePixiOperator", "BasePixiPythonOperator", "PixiOperator"]

DEFAULT_CHANNELS = ("conda-forge",)
DEFAULT_PLATFORMS = ("linux-64", "osx-64", "osx-arm64", "win-64")


def _write_atomic(path: str, text: str) -> None:
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".pixi-airflow-")
    with os.fdopen(fd, "w") as f:
        f.write(text)
    os.replace(tmp, path)


def _terminate(proc: subprocess.Popen[bytes]) -> None:
    """Stop pixi and everything it started; it runs as the leader of its own process group."""
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
        # whatever ignored SIGTERM, including children of an exited pixi
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
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
    :param requirements: pip requirement strings, as ``@task.virtualenv`` takes them (templated), added to an
        inline manifest's ``[pypi-dependencies]``. An inline manifest with PyPI packages but no ``python``
        dependency gets the worker's Python version.
    :param dependencies: conda dependencies of an inline manifest, a dict or a list of MatchSpecs.
    :param pypi_dependencies: ``[pypi-dependencies]`` of an inline manifest.
    :param channels: channels of an inline manifest; default ``["conda-forge"]``.
    :param platforms: platforms of an inline manifest; default ``linux-64``, ``osx-64``, ``osx-arm64``,
        ``win-64``.
    :param workspace_name: ``[workspace] name`` of an inline manifest.
    :param pypi_options: ``[pypi-options]`` of an inline manifest.
    :param environments: ``[environments]`` of an inline manifest.
    :param feature: ``[feature.<name>]`` tables of an inline manifest, with ``channels``, ``platforms``,
        ``dependencies`` and ``pypi_dependencies``.
    :param env_cache_path: keep inline environments in ``<env_cache_path>/pixi-<hash>`` and reuse them across
        runs, like ``venv_cache_path`` of ``PythonVirtualenvOperator``. Otherwise an inline environment is
        built in a temporary directory, removed after the run unless ``cleanup_temp_manifest=False``.
    :param cleanup_temp_manifest: remove the temporary directory of an inline environment after the run.
    :param pixi_binary: the pixi executable, a name on ``PATH`` or a path. It must be installed on the workers,
        in version :data:`MIN_PIXI_VERSION` or newer; the provider never installs it.
    """

    template_fields: Sequence[str] = ("pixi_project_path", "pixi_toml_path", "environment", "requirements")

    def __init__(
        self,
        *,
        pixi_project_path: str | None = None,
        pixi_toml_path: str | None = None,
        environment: str | None = None,
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
        pixi_binary: str = "pixi",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.pixi_project_path = pixi_project_path
        self.pixi_toml_path = pixi_toml_path
        self.environment = environment
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
        self.pixi_binary = pixi_binary
        self._held_manifest: tuple[str, str] | None = None

        has_inline = bool(dependencies) or bool(pypi_dependencies) or bool(self.requirements)
        if sum([pixi_project_path is not None, pixi_toml_path is not None, has_inline]) != 1:
            raise ValueError(
                "Exactly one of pixi_project_path, pixi_toml_path, or inline "
                "(dependencies / pypi_dependencies / requirements) must be provided."
            )
        if not any("{{" in r for r in self.requirements):  # templated ones are checked when they are rendered
            pypi_dependencies_from_requirements(self.requirements)

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
            # pixi installs PyPI packages only with a conda Python; default to the worker's, as virtualenvs do
            dependencies["python"] = f"{sys.version_info.major}.{sys.version_info.minor}.*"
        return build_pixi_toml(
            channels=self.channels or DEFAULT_CHANNELS,
            platforms=self.platforms or DEFAULT_PLATFORMS,
            workspace_name=self.workspace_name,
            dependencies=dependencies,
            pypi_dependencies=pypi_dependencies,
            pypi_options=self.pypi_options,
            environments=self.environments,
            feature=self.feature,
        )

    def pixi_run_command(self, pixi: str, manifest: str) -> list[str]:
        """Return ``pixi run`` with the manifest and environment, ready for the command to run."""
        command = [pixi, "run", "--manifest-path", manifest]
        if self.environment:
            command += ["--environment", self.environment]
        return command

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
        unless ``cleanup_temp_manifest=False``. Runs at execution time, so templated paths are rendered.
        """
        if self._held_manifest is not None:
            yield self._held_manifest
            return
        if self.pixi_project_path is not None:
            project = os.path.abspath(self.pixi_project_path)
            self._check_project_manifest(project)
            yield project, project
            return
        if self.pixi_toml_path is not None:
            manifest = os.path.abspath(self.pixi_toml_path)
            self._check_project_manifest(manifest)
            yield manifest, os.path.dirname(manifest)
            return
        toml = self.inline_manifest_toml()
        if self.env_cache_path:
            digest = hashlib.sha256(toml.encode()).hexdigest()[:16]
            env_dir = os.path.join(os.path.abspath(self.env_cache_path), f"pixi-{digest}")
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
        decorator built on ``@task.pixi``), and line numbers kept, so tracebacks point at the DAG file.
        Override it to wrap the function, for example by appending code that rebinds the name to a wrapper,
        as for ``@task.virtualenv``.
        """
        stripped = set(STRIPPED_DECORATORS)
        operator_name = getattr(self, "custom_operator_name", "")
        if operator_name.startswith("@"):
            stripped.add(operator_name[1:])
        return function_source(self.python_callable, stripped)[0]  # type: ignore[arg-type]

    def callable_input(self) -> bytes:
        """Return the runner's input: the callable and its arguments, in the ``serializer``'s format."""
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
        if self.serializer == "pickle":
            return pickle.dumps(spec, protocol=PICKLE_PROTOCOL)
        try:
            return json.dumps(spec).encode()
        except TypeError as e:
            raise TypeError(f"op_args and op_kwargs must be JSON-serializable, or pass serializer='pickle': {e}") from e

    def _missing_result(self) -> AirflowException:
        name = getattr(self.python_callable, "__name__", self.python_callable)
        return AirflowException(f"{name} exited without returning, for example through sys.exit()")


class PixiSubprocessMixin(BaseOperator):
    """Run the runner with pixi on the worker, streaming its output to the task log.

    :param env_vars: environment variables for the run, on top of the worker's. Not templated, so secrets
        set here are never rendered into the UI.
    :param pixi_cache_dir_variable: name of an Airflow Variable holding ``PIXI_CACHE_DIR`` for the run;
        likewise ``uv_cache_dir_variable`` (``UV_CACHE_DIR``) and ``pip_cache_dir_variable`` (``PIP_CACHE_DIR``).

    ``execution_timeout`` and killing the task stop pixi and everything it started.
    """

    template_fields: Sequence[str] = ("pixi_cache_dir_variable", "uv_cache_dir_variable", "pip_cache_dir_variable")

    def __init__(
        self,
        *,
        env_vars: dict[str, str] | None = None,
        pixi_cache_dir_variable: str | None = None,
        uv_cache_dir_variable: str | None = None,
        pip_cache_dir_variable: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.env_vars = env_vars
        self.pixi_cache_dir_variable = pixi_cache_dir_variable
        self.uv_cache_dir_variable = uv_cache_dir_variable
        self.pip_cache_dir_variable = pip_cache_dir_variable
        self._process: subprocess.Popen[bytes] | None = None

    def run_callable(self: Any) -> Any:
        """Call ``python_callable`` in the environment and return its result."""
        pixi = resolve_pixi(self.pixi_binary)
        with self.local_manifest() as (manifest, cwd), tempfile.TemporaryDirectory(prefix="airflow_pixi_io_") as io:
            input_path, output_path = os.path.join(io, "input"), os.path.join(io, "output")
            Path(input_path).write_bytes(self.callable_input())
            command = self.pixi_run_command(pixi, manifest)
            self.log.info("Running %s in %s", " ".join(command), cwd)
            self._run([*command, "python", "-c", RUNNER_SCRIPT, self.serializer, input_path, output_path], cwd)
            if not os.path.exists(output_path):
                raise self._missing_result()
            if self.serializer == "pickle":
                return pickle.loads(Path(output_path).read_bytes())
            return json.loads(Path(output_path).read_text())

    def on_kill(self) -> None:
        if self._process is not None:
            _terminate(self._process)

    def _env(self) -> dict[str, str]:
        env = os.environ.copy()
        env.setdefault("PYTHONUNBUFFERED", "1")  # the callable's output reaches the log as it prints
        for variable, env_var in (
            (self.pixi_cache_dir_variable, "PIXI_CACHE_DIR"),
            (self.uv_cache_dir_variable, "UV_CACHE_DIR"),
            (self.pip_cache_dir_variable, "PIP_CACHE_DIR"),
        ):
            if variable:
                path = Variable.get(variable, default=None)
                if path is not None:
                    env[env_var] = str(path).strip()
        env.update(self.env_vars or {})
        return env

    def _run(self, cmd: list[str], cwd: str) -> None:
        tail: collections.deque[str] = collections.deque(maxlen=20)
        with subprocess.Popen(
            cmd,
            cwd=cwd,
            env=self._env(),
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
        if returncode != 0:
            raise AirflowException(f"pixi run exited with code {returncode}:\n" + "\n".join(tail))


class PixiOperator(PixiSubprocessMixin, BasePixiPythonOperator):
    """Run a Python callable inside a Pixi environment on the worker.

    Accepts every :class:`BasePixiPythonOperator` and :class:`BasePixiOperator` argument, plus ``env_vars``
    and the cache directory Variables of :class:`PixiSubprocessMixin`. The return value is the task's
    XCom. The output of the run is streamed to the task log; ``execution_timeout`` and killing the task
    stop pixi and everything it started.

    Subclasses and mixins can change what runs, the same way as for ``@task.virtualenv``: override
    :meth:`get_python_source`, and set ``op_args``, ``op_kwargs``, ``env_vars`` or ``requirements`` in
    ``execute`` before calling ``super().execute``.
    """

    template_fields: Sequence[str] = (*BasePixiPythonOperator.template_fields, *PixiSubprocessMixin.template_fields)
    custom_operator_name = "Pixi"

    def execute(self, context: Any) -> Any:
        return self.run_callable()
