"""Run a Python callable inside a Pixi environment."""

from __future__ import annotations

import ast
import collections
import hashlib
import inspect
import json
import os
import pickle
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import textwrap
from collections.abc import Callable, Collection, Sequence
from pathlib import Path
from typing import Any, ClassVar, Literal

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator, Variable
from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

# Runs inside the Pixi environment as `python -c RUNNER_SCRIPT <serializer> <input> <output>`. The input
# file holds the callable (a module path or a function's source) and its arguments; the return value goes
# to the output file, which keeps stdout and stderr free for the task log. Must run on old Pythons too.
RUNNER_SCRIPT = """
import importlib, json, pickle, sys

serializer, input_path, output_path = sys.argv[1:4]
if serializer == "pickle":
    with open(input_path, "rb") as f:
        spec = pickle.load(f)
else:
    with open(input_path) as f:
        spec = json.load(f)
if "source" in spec:
    namespace = {"__name__": "__pixi_airflow_task__"}
    exec(compile(spec["source"], spec["filename"], "exec"), namespace)
    fn = namespace[spec["name"]]
else:
    fn = getattr(importlib.import_module(spec["module"]), spec["name"])
result = fn(*spec["args"], **spec["kwargs"])
if serializer == "pickle":
    with open(output_path, "wb") as f:
        pickle.dump(result, f, protocol=4)
else:
    try:
        data = json.dumps(result)
    except TypeError as e:
        raise TypeError(str(e) + "; return a JSON-serializable value or pass serializer='pickle'") from None
    with open(output_path, "w") as f:
        f.write(data)
"""

# Decorators Airflow strips when it ships a function's source (as @task.virtualenv does), plus ours.
_STRIPPED_DECORATORS = {"setup", "teardown", "task.skip_if", "task.run_if", "task.pixi", "pixi_task"}

# pickle protocol readable by every Python 3.4+, since the environment may run an older Python
_PICKLE_PROTOCOL = 4


def _ensure_pixi_available(pixi_binary: str, auto_install: bool = True) -> str:
    """
    Return path to pixi executable. If not on PATH and auto_install is True,
    run the official install script and return the install path.
    """
    resolved = shutil.which(pixi_binary)
    if resolved:
        return resolved
    if not auto_install:
        raise AirflowException(
            f"Pixi binary '{pixi_binary}' not found on PATH. "
            "Install from https://pixi.sh or set auto_install_pixi=True."
        )
    # Run official install script
    if sys.platform == "win32":
        install_dir = Path(os.environ.get("LOCALAPPDATA", "")) / "pixi" / "bin"
        cmd_install = [
            "powershell",
            "-ExecutionPolicy",
            "ByPass",
            "-c",
            "irm -useb https://pixi.sh/install.ps1 | iex",
        ]
    else:
        install_dir = Path.home() / ".pixi" / "bin"
        cmd_install = ["sh", "-c", "curl -fsSL https://pixi.sh/install.sh | sh"]
    proc = subprocess.run(
        cmd_install,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    if proc.returncode != 0:
        raise AirflowException(f"Pixi auto-install failed (exit {proc.returncode}): {proc.stderr or proc.stdout}")
    pixi_path = install_dir / "pixi.exe" if sys.platform == "win32" else install_dir / "pixi"
    if not pixi_path.is_file():
        raise AirflowException(f"Pixi install completed but binary not found at {pixi_path}")
    return str(pixi_path)


def _validate_callable(python_callable: Callable[..., Any] | str) -> None:
    if isinstance(python_callable, str):
        module, sep, name = python_callable.partition(":")
        if not (module and sep and name) or ":" in name:
            raise ValueError("python_callable string must be in format 'module.path:callable_name'")
    elif not inspect.isfunction(inspect.unwrap(python_callable)) or python_callable.__name__ == "<lambda>":
        raise ValueError(
            "python_callable must be a function defined with def, whose source is shipped to the Pixi "
            "environment, or a 'module.path:callable_name' string importable there"
        )


def _decorator_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Call):
        return _decorator_name(node.func)
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        return f"{node.value.id}.{node.attr}"
    return None


def _function_source(fn: Callable[..., Any], stripped: Collection[str] = _STRIPPED_DECORATORS) -> tuple[str, str]:
    """Return the source of ``fn`` without the ``stripped`` decorators, and the file it comes from.

    Stripped decorator lines become blank lines and the source is padded to its first line, so
    tracebacks raised inside the Pixi environment point at the right lines of the file.
    """
    fn = inspect.unwrap(fn)
    lines, start = inspect.getsourcelines(fn)
    source = textwrap.dedent("".join(lines))
    func = ast.parse(source).body[0]
    stripped = {
        line
        for decorator in getattr(func, "decorator_list", [])
        if _decorator_name(decorator) in stripped
        for line in range(decorator.lineno - 1, decorator.end_lineno or decorator.lineno)
    }
    body = "".join("\n" if i in stripped else line for i, line in enumerate(source.splitlines(keepends=True)))
    return "\n" * (start - 1) + body, inspect.getsourcefile(fn) or "<pixi task>"


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


_REQUIREMENT_COMMENT = re.compile(r"(^|\s)#.*$")
# a conda MatchSpec: name, optionally with a channel (``conda-forge::numpy``), then the version
_MATCHSPEC = re.compile(r"^\s*(?:(?P<channel>[^:\s]+)::)?(?P<name>[A-Za-z0-9_.\-]+)\s*(?P<version>.*?)\s*$")


def _toml_key(key: Any) -> str:
    return json.dumps(str(key))


def _toml_value(value: Any) -> str:
    """Render ``value`` as TOML. JSON strings, numbers, booleans and arrays are valid TOML; tables are not."""
    if isinstance(value, dict):
        return "{ " + ", ".join(f"{_toml_key(k)} = {_toml_value(v)}" for k, v in value.items()) + " }"
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_toml_value(v) for v in value) + "]"
    if isinstance(value, (str, bool, int, float)):
        return json.dumps(value)
    raise TypeError(f"cannot write {value!r} to a pixi manifest")


def _table(lines: list[str], header: str, entries: dict[str, Any]) -> None:
    lines.append(f"[{header}]")
    lines.extend(f"{_toml_key(k)} = {_toml_value(v)}" for k, v in entries.items())
    lines.append("")


def _conda_dependencies(dependencies: dict[str, Any] | Sequence[str] | None) -> dict[str, Any]:
    """``[dependencies]`` from a dict, or from a list of MatchSpecs such as ``"numpy>=2"``."""
    if dependencies is None or isinstance(dependencies, dict):
        return dict(dependencies or {})
    result: dict[str, Any] = {}
    for spec in dependencies:
        match = _MATCHSPEC.match(spec)
        if match is None:
            raise ValueError(f"invalid conda MatchSpec {spec!r}")
        version = match["version"] or "*"
        result[match["name"]] = {"version": version, "channel": match["channel"]} if match["channel"] else version
    return result


def _pypi_spec_from_url(url: str) -> dict[str, str]:
    if url.startswith("git+"):
        repository = url[len("git+") :]
        # a revision follows the last "@" of the path, not the one of ``ssh://git@host``
        at = repository.rfind("@")
        if at > repository.rfind("/"):
            return {"git": repository[:at], "rev": repository[at + 1 :]}
        return {"git": repository}
    if url.startswith("file://"):
        return {"path": url[len("file://") :]}
    return {"url": url}


def _pypi_dependencies_from_requirements(requirements: Sequence[str]) -> dict[str, Any]:
    """``[pypi-dependencies]`` for pip requirement lines, as ``@task.virtualenv`` takes them.

    An element may hold several lines, such as a rendered requirements file; blank lines and comments are
    skipped. Pip options and environment markers have no equivalent in an inline manifest.
    """
    result: dict[str, Any] = {}
    for item in requirements:
        for raw in str(item).splitlines():
            line = _REQUIREMENT_COMMENT.sub("", raw).strip()
            if not line:
                continue
            if line.startswith("-"):
                raise ValueError(f"requirements cannot hold pip options such as {line!r}; use pypi_options")
            try:
                requirement = Requirement(line)
            except InvalidRequirement as e:
                raise ValueError(f"invalid requirement {line!r}: {e}") from None
            if requirement.marker is not None:
                raise ValueError(f"requirement {line!r}: environment markers are not supported")
            if any(canonicalize_name(name) == canonicalize_name(requirement.name) for name in result):
                raise ValueError(f"{requirement.name} is listed twice in requirements")
            spec: Any = str(requirement.specifier) or "*"
            if requirement.url:
                spec = _pypi_spec_from_url(requirement.url)
            if requirement.extras:
                spec = {**(spec if isinstance(spec, dict) else {"version": spec}), "extras": sorted(requirement.extras)}
            result[requirement.name] = spec
    return result


def _build_pixi_toml(
    *,
    channels: Sequence[str],
    platforms: Sequence[str],
    name: str | None = None,
    dependencies: dict[str, Any] | Sequence[str] | None = None,
    pypi_dependencies: dict[str, Any] | None = None,
    pypi_options: dict[str, Any] | None = None,
    environments: dict[str, Any] | None = None,
    feature: dict[str, Any] | None = None,
) -> str:
    """Build pixi.toml content from inline config (same options as pixi.toml format)."""
    lines = ["[workspace]", "channels = " + _toml_value(list(channels)), "platforms = " + _toml_value(list(platforms))]
    if name:
        lines.append("name = " + _toml_value(name))
    lines.append("")
    if dependencies:
        _table(lines, "dependencies", _conda_dependencies(dependencies))
    if pypi_dependencies:
        _table(lines, "pypi-dependencies", pypi_dependencies)
    if pypi_options:
        _table(lines, "pypi-options", pypi_options)
    for feat_name, feat_cfg in (feature or {}).items():
        if not isinstance(feat_cfg, dict):
            continue
        header = f"feature.{_toml_key(feat_name)}"
        _table(lines, header, {k: feat_cfg[k] for k in ("channels", "platforms") if k in feat_cfg})
        if "dependencies" in feat_cfg:
            _table(lines, f"{header}.dependencies", _conda_dependencies(feat_cfg["dependencies"]))
        if "pypi_dependencies" in feat_cfg:
            _table(lines, f"{header}.pypi-dependencies", feat_cfg["pypi_dependencies"])
    if environments:
        _table(lines, "environments", environments)
    return "\n".join(lines)


class PixiOperator(BaseOperator):
    """Run a Python callable inside a Pixi environment.

    The environment comes from exactly one of ``pixi_project_path`` (a project directory),
    ``pixi_toml_path`` (a ``pixi.toml`` or ``pyproject.toml``) or an inline manifest:
    ``dependencies`` / ``pypi_dependencies`` / ``requirements`` plus ``channels``, ``platforms``,
    ``name``, ``pypi_options``, ``environments`` and ``feature``, as in ``pixi.toml``.

    :param python_callable: a function, whose source is shipped to the environment and run there, so it
        must be self-contained (imports inside it, no variables from enclosing functions); or a
        ``"module.path:callable_name"`` string imported in the environment. The manifest's directory is the
        working directory, so modules next to it are importable.
    :param op_args: positional arguments for the callable (templated).
    :param op_kwargs: keyword arguments for the callable (templated).
    :param requirements: pip requirement strings, as ``@task.virtualenv`` takes them (templated), added to an
        inline manifest's ``[pypi-dependencies]``. An inline manifest with PyPI packages but no ``python``
        dependency gets the worker's Python version.
    :param env_vars: environment variables for the run, on top of the worker's. Not templated, so secrets
        set here are never rendered into the UI.
    :param environment: the manifest environment to run in, if it defines several.
    :param serializer: how arguments and the return value travel: ``"json"`` (default), or ``"pickle"`` for
        values JSON cannot hold, whose types must then be importable on both sides.
    :param env_cache_path: keep inline environments in ``<env_cache_path>/pixi-<hash>`` and reuse them across
        runs, like ``venv_cache_path`` of ``PythonVirtualenvOperator``. Otherwise an inline environment is
        built in a temporary directory, removed after the run unless ``cleanup_temp_manifest=False``.
    :param pixi_binary: the pixi executable. Installed with the official script when missing, unless
        ``auto_install_pixi=False``.
    :param pixi_cache_dir_variable: name of an Airflow Variable holding ``PIXI_CACHE_DIR`` for the run;
        likewise ``uv_cache_dir_variable`` (``UV_CACHE_DIR``) and ``pip_cache_dir_variable`` (``PIP_CACHE_DIR``).

    The output of the run is streamed to the task log. ``execution_timeout`` and killing the task stop pixi
    and everything it started.

    Subclasses and mixins can change what runs, the same way as for ``@task.virtualenv``: override
    :meth:`get_python_source`, and set ``op_args``, ``op_kwargs``, ``env_vars`` or ``requirements`` in
    ``execute`` before calling ``super().execute``.
    """

    template_fields: Sequence[str] = (
        "op_args",
        "op_kwargs",
        "pixi_project_path",
        "pixi_toml_path",
        "requirements",
        "pixi_cache_dir_variable",
        "uv_cache_dir_variable",
        "pip_cache_dir_variable",
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {"op_args": "py", "op_kwargs": "py"}
    custom_operator_name = "Pixi"

    def __init__(
        self,
        *,
        pixi_project_path: str | None = None,
        pixi_toml_path: str | None = None,
        python_callable: Callable[..., Any] | str,
        op_args: Sequence[Any] | None = None,
        op_kwargs: dict[str, Any] | None = None,
        requirements: Sequence[str] | str | None = None,
        env_vars: dict[str, str] | None = None,
        environment: str | None = None,
        serializer: Literal["json", "pickle"] = "json",
        env_cache_path: str | None = None,
        pixi_binary: str = "pixi",
        auto_install_pixi: bool = True,
        cleanup_temp_manifest: bool = True,
        # Cache dirs from Airflow Variables (-> PIXI_CACHE_DIR, UV_CACHE_DIR, PIP_CACHE_DIR)
        pixi_cache_dir_variable: str | None = None,
        uv_cache_dir_variable: str | None = None,
        pip_cache_dir_variable: str | None = None,
        # Inline manifest (mutually exclusive with path/toml_path)
        dependencies: dict[str, Any] | Sequence[str] | None = None,
        pypi_dependencies: dict[str, Any] | None = None,
        channels: Sequence[str] | None = None,
        platforms: Sequence[str] | None = None,
        name: str | None = None,
        pypi_options: dict[str, Any] | None = None,
        environments: dict[str, Any] | None = None,
        feature: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.pixi_project_path = pixi_project_path
        self.pixi_toml_path = pixi_toml_path
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.requirements = [requirements] if isinstance(requirements, str) else list(requirements or [])
        self.env_vars = env_vars
        self.environment = environment
        self.serializer = serializer
        self.env_cache_path = env_cache_path
        self.pixi_binary = pixi_binary
        self.auto_install_pixi = auto_install_pixi
        self.cleanup_temp_manifest = cleanup_temp_manifest
        self.dependencies = dependencies
        self.pypi_dependencies = pypi_dependencies
        self.channels = channels
        self.platforms = platforms
        self._name = name
        self.pypi_options = pypi_options
        self.environments = environments
        self.feature = feature
        self.pixi_cache_dir_variable = pixi_cache_dir_variable
        self.uv_cache_dir_variable = uv_cache_dir_variable
        self.pip_cache_dir_variable = pip_cache_dir_variable
        self._process: subprocess.Popen[bytes] | None = None

        # Validate exactly one manifest source
        has_path = pixi_project_path is not None
        has_toml = pixi_toml_path is not None
        has_inline = bool(dependencies) or bool(pypi_dependencies) or bool(self.requirements)
        if sum([has_path, has_toml, has_inline]) != 1:
            raise ValueError(
                "Exactly one of pixi_project_path, pixi_toml_path, or inline "
                "(dependencies / pypi_dependencies / requirements) must be provided."
            )
        if not any("{{" in r for r in self.requirements):  # templated ones are checked when they are rendered
            _pypi_dependencies_from_requirements(self.requirements)
        if serializer not in ("json", "pickle"):
            raise ValueError(f"serializer must be 'json' or 'pickle', not {serializer!r}")
        _validate_callable(python_callable)
        if not isinstance(python_callable, str) and (free := inspect.unwrap(python_callable).__code__.co_freevars):
            raise ValueError(
                f"{python_callable.__name__} uses {', '.join(free)} from an enclosing function, which does not "
                "exist in the Pixi environment; pass it as an argument instead"
            )

    @property
    def inline_manifest(self) -> bool:
        """Whether the environment comes from an inline manifest, the only one ``requirements`` can extend."""
        return self.pixi_project_path is None and self.pixi_toml_path is None

    def get_python_source(self) -> str:
        """Return the source shipped to the environment when ``python_callable`` is a function.

        The runner executes it and calls the name of ``python_callable`` in the resulting namespace. Task
        decorators are removed, including the one named by ``custom_operator_name`` (``@task.<name>`` of a
        decorator built on ``@task.pixi``), and line numbers kept, so tracebacks point at the DAG file.
        Override it to wrap the function, for example by appending code that rebinds the name to a wrapper,
        as for ``@task.virtualenv``.
        """
        stripped = set(_STRIPPED_DECORATORS)
        if self.custom_operator_name.startswith("@"):
            stripped.add(self.custom_operator_name[1:])
        return _function_source(self.python_callable, stripped)[0]  # type: ignore[arg-type]

    def execute(self, context: Any) -> Any:
        pixi = _ensure_pixi_available(self.pixi_binary, auto_install=self.auto_install_pixi)
        manifest, cwd, temp_env_dir = self._manifest()
        try:
            with tempfile.TemporaryDirectory(prefix="airflow_pixi_io_") as io_dir:
                input_path = os.path.join(io_dir, "input")
                output_path = os.path.join(io_dir, "output")
                self._write_input(input_path)
                cmd = [pixi, "run", "--manifest-path", manifest]
                if self.environment:
                    cmd += ["--environment", self.environment]
                self.log.info("Running %s in %s", " ".join(cmd), cwd)
                self._run([*cmd, "python", "-c", RUNNER_SCRIPT, self.serializer, input_path, output_path], cwd)
                return self._read_output(output_path)
        finally:
            if temp_env_dir is not None:
                shutil.rmtree(temp_env_dir, ignore_errors=True)

    def on_kill(self) -> None:
        if self._process is not None:
            _terminate(self._process)

    def _manifest(self) -> tuple[str, str, str | None]:
        """Return the manifest to pass to pixi, the working directory and a directory to remove afterwards.

        Runs at execution time, so templated paths are already rendered.
        """
        if self.pixi_project_path is not None:
            project = os.path.abspath(self.pixi_project_path)
            return self._project_manifest(project, project)
        if self.pixi_toml_path is not None:
            manifest = os.path.abspath(self.pixi_toml_path)
            return self._project_manifest(manifest, os.path.dirname(manifest))
        pypi_dependencies = self._pypi_dependencies()
        dependencies = _conda_dependencies(self.dependencies)
        if pypi_dependencies and "python" not in dependencies and not self.feature:
            # pixi installs PyPI packages only with a conda Python; default to the worker's, as virtualenvs do
            dependencies["python"] = f"{sys.version_info.major}.{sys.version_info.minor}.*"
        toml = _build_pixi_toml(
            channels=self.channels or ["conda-forge"],
            platforms=self.platforms or ["linux-64", "osx-64", "osx-arm64", "win-64"],
            name=self._name,
            dependencies=dependencies,
            pypi_dependencies=pypi_dependencies,
            pypi_options=self.pypi_options,
            environments=self.environments,
            feature=self.feature,
        )
        if self.env_cache_path:
            digest = hashlib.sha256(toml.encode()).hexdigest()[:16]
            env_dir = os.path.join(os.path.abspath(self.env_cache_path), f"pixi-{digest}")
            os.makedirs(env_dir, exist_ok=True)
            manifest = os.path.join(env_dir, "pixi.toml")
            if not os.path.exists(manifest):
                _write_atomic(manifest, toml)
            return manifest, env_dir, None
        env_dir = tempfile.mkdtemp(prefix="airflow_pixi_")
        manifest = os.path.join(env_dir, "pixi.toml")
        Path(manifest).write_text(toml)
        return manifest, env_dir, env_dir if self.cleanup_temp_manifest else None

    def _project_manifest(self, manifest: str, cwd: str) -> tuple[str, str, None]:
        if self.requirements:
            raise AirflowException(
                f"requirements {self.requirements} can only extend an inline manifest; "
                f"add them to the environment of {manifest} instead"
            )
        return manifest, cwd, None

    def _pypi_dependencies(self) -> dict[str, Any]:
        """``pypi_dependencies`` together with ``requirements``."""
        result = dict(self.pypi_dependencies or {})
        listed = {canonicalize_name(name) for name in result}
        for name, spec in _pypi_dependencies_from_requirements(self.requirements).items():
            if canonicalize_name(name) in listed:
                raise ValueError(f"{name} is listed in both pypi_dependencies and requirements")
            result[name] = spec
        return result

    def _write_input(self, path: str) -> None:
        if isinstance(self.python_callable, str):
            module, _, name = self.python_callable.partition(":")
            callable_spec = {"module": module, "name": name}
        else:
            fn = inspect.unwrap(self.python_callable)
            callable_spec = {
                "source": self.get_python_source(),
                "filename": inspect.getsourcefile(fn) or "<pixi task>",
                "name": fn.__name__,
            }
        spec = {**callable_spec, "args": list(self.op_args), "kwargs": dict(self.op_kwargs)}
        if self.serializer == "pickle":
            with open(path, "wb") as f:
                pickle.dump(spec, f, protocol=_PICKLE_PROTOCOL)
            return
        try:
            data = json.dumps(spec)
        except TypeError as e:
            raise TypeError(f"op_args and op_kwargs must be JSON-serializable, or pass serializer='pickle': {e}") from e
        Path(path).write_text(data)

    def _read_output(self, path: str) -> Any:
        if not os.path.exists(path):
            name = getattr(self.python_callable, "__name__", self.python_callable)
            raise AirflowException(f"{name} exited without returning, for example through sys.exit()")
        if self.serializer == "pickle":
            with open(path, "rb") as f:
                return pickle.load(f)
        return json.loads(Path(path).read_text())

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
