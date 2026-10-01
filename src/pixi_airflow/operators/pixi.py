"""Run a Python callable inside a Pixi environment."""

from __future__ import annotations

import inspect
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models.variable import Variable
from airflow.sdk import BaseOperator

# Runner script: reads JSON path from env AIRFLOW_PIXI_ARGS_FILE, loads module/callable/args/kwargs,
# runs callable, prints result as JSON on last line (single line) for operator to parse.
RUNNER_SCRIPT = """
import json, os, sys
p = os.environ.get("AIRFLOW_PIXI_ARGS_FILE") or (sys.argv[1] if len(sys.argv) > 1 else None)
if not p or not os.path.isfile(p):
    raise SystemExit("Missing or invalid args file")
with open(p) as f:
    d = json.load(f)
mod = __import__(d["module"], fromlist=[d["callable"]])
fn = getattr(mod, d["callable"])
r = fn(*(d.get("args") or []), **(d.get("kwargs") or {}))
print(json.dumps(r, default=str))
"""


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


def _resolve_callable_ref(python_callable: Callable[..., Any] | str) -> str:
    """Resolve python_callable to a 'module:name' string."""
    if isinstance(python_callable, str):
        if ":" not in python_callable or python_callable.count(":") != 1:
            raise ValueError("python_callable string must be in format 'module.path:callable_name'")
        return python_callable
    mod = inspect.getmodule(python_callable)
    if mod is None:
        raise ValueError("Could not resolve module for callable")
    return f"{mod.__name__}:{python_callable.__name__}"


def _build_pixi_toml(
    *,
    channels: Sequence[str],
    platforms: Sequence[str],
    name: str | None = None,
    dependencies: dict[str, Any] | list[str] | None = None,
    pypi_dependencies: dict[str, Any] | None = None,
    pypi_options: dict[str, Any] | None = None,
    environments: dict[str, Any] | None = None,
    feature: dict[str, Any] | None = None,
) -> str:
    """Build pixi.toml content from inline config (same options as pixi.toml format)."""
    lines: list[str] = []
    lines.append("[workspace]")
    lines.append("channels = " + json.dumps(list(channels)))
    lines.append("platforms = " + json.dumps(list(platforms)))
    if name:
        lines.append(f'name = "{name}"')
    lines.append("")

    if dependencies:
        lines.append("[dependencies]")
        if isinstance(dependencies, list):
            for dep in dependencies:
                lines.append(f'"{dep}" = "*"')
        else:
            for k, v in dependencies.items():
                if isinstance(v, str):
                    lines.append(f'"{k}" = "{v}"')
                else:
                    lines.append(f'"{k}" = {json.dumps(v)}')
        lines.append("")

    if pypi_dependencies:
        lines.append("[pypi-dependencies]")
        for k, v in pypi_dependencies.items():
            if isinstance(v, str):
                lines.append(f'"{k}" = "{v}"')
            else:
                lines.append(f'"{k}" = {json.dumps(v)}')
        lines.append("")

    if pypi_options:
        lines.append("[pypi-options]")
        for k, v in pypi_options.items():
            if isinstance(v, str):
                lines.append(f'{k} = "{v}"')
            else:
                lines.append(f"{k} = {json.dumps(v)}")
        lines.append("")

    if feature:
        for feat_name, feat_cfg in feature.items():
            if not isinstance(feat_cfg, dict):
                continue
            lines.append(f"[feature.{feat_name}]")
            if "channels" in feat_cfg:
                lines.append("channels = " + json.dumps(feat_cfg["channels"]))
            if "platforms" in feat_cfg:
                lines.append("platforms = " + json.dumps(feat_cfg["platforms"]))
            if "dependencies" in feat_cfg:
                lines.append("")
                lines.append(f"[feature.{feat_name}.dependencies]")
                for k, v in feat_cfg["dependencies"].items():
                    if isinstance(v, str):
                        lines.append(f'"{k}" = "{v}"')
                    else:
                        lines.append(f'"{k}" = {json.dumps(v)}')
            if "pypi_dependencies" in feat_cfg:
                lines.append("")
                lines.append(f"[feature.{feat_name}.pypi-dependencies]")
                for k, v in feat_cfg["pypi_dependencies"].items():
                    if isinstance(v, str):
                        lines.append(f'"{k}" = "{v}"')
                    else:
                        lines.append(f'"{k}" = {json.dumps(v)}')
            lines.append("")

    if environments:
        lines.append("[environments]")
        for env_name, env_val in environments.items():
            if isinstance(env_val, list):
                lines.append(f"{env_name} = {json.dumps(env_val)}")
            else:
                lines.append(f"{env_name} = {json.dumps(env_val)}")
        lines.append("")

    return "\n".join(lines)


class PixiOperator(BaseOperator):
    """
    Run a Python callable inside a Pixi environment.

    Manifest can be specified as: pixi_project_path (dir), pixi_toml_path (file),
    or inline via dependencies/pypi_dependencies/channels/platforms/environments/feature.
    """

    template_fields: Sequence[str] = (
        "pixi_project_path",
        "pixi_toml_path",
        "pixi_cache_dir_variable",
        "uv_cache_dir_variable",
        "pip_cache_dir_variable",
    )
    custom_operator_name = "Pixi"

    def __init__(
        self,
        *,
        pixi_project_path: str | None = None,
        pixi_toml_path: str | None = None,
        python_callable: Callable[..., Any] | str,
        op_args: Sequence[Any] | None = None,
        op_kwargs: dict[str, Any] | None = None,
        environment: str | None = None,
        pixi_binary: str = "pixi",
        auto_install_pixi: bool = True,
        cleanup_temp_manifest: bool = True,
        # Cache dirs from Airflow Variables (-> PIXI_CACHE_DIR, UV_CACHE_DIR, PIP_CACHE_DIR)
        pixi_cache_dir_variable: str | None = None,
        uv_cache_dir_variable: str | None = None,
        pip_cache_dir_variable: str | None = None,
        # Inline manifest (mutually exclusive with path/toml_path)
        dependencies: dict[str, Any] | list[str] | None = None,
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
        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}
        self.environment = environment
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

        # Validate exactly one manifest source
        has_path = pixi_project_path is not None
        has_toml = pixi_toml_path is not None
        has_inline = (dependencies is not None and len(dependencies) > 0) or (
            pypi_dependencies is not None and len(pypi_dependencies) > 0
        )
        if sum([has_path, has_toml, has_inline]) != 1:
            raise ValueError(
                "Exactly one of pixi_project_path, pixi_toml_path, or inline "
                "(dependencies / pypi_dependencies) must be provided."
            )

        self._callable_ref = _resolve_callable_ref(python_callable)
        self._manifest_dir: str | None = None
        if pixi_project_path is not None:
            self._manifest_dir = os.path.abspath(pixi_project_path)
        elif pixi_toml_path is not None:
            self._manifest_dir = os.path.abspath(os.path.dirname(pixi_toml_path))
        # For inline, _manifest_dir is set in execute()

    def execute(self, context: Any) -> Any:
        import tempfile as tf

        temp_dir: str | None = None
        try:
            if self._manifest_dir is None:
                # Inline: create temp dir and write pixi.toml
                channels = self.channels or ["conda-forge"]
                platforms = self.platforms or [
                    "linux-64",
                    "osx-64",
                    "osx-arm64",
                    "win-64",
                ]
                temp_dir = tf.mkdtemp(prefix="airflow_pixi_")
                self._manifest_dir = temp_dir
                toml_content = _build_pixi_toml(
                    channels=channels,
                    platforms=platforms,
                    name=self._name,
                    dependencies=self.dependencies,
                    pypi_dependencies=self.pypi_dependencies,
                    pypi_options=self.pypi_options,
                    environments=self.environments,
                    feature=self.feature,
                )
                with open(os.path.join(temp_dir, "pixi.toml"), "w") as f:
                    f.write(toml_content)

            args_data = {
                "module": self._callable_ref.split(":", 1)[0],
                "callable": self._callable_ref.split(":", 1)[1],
                "args": list(self.op_args),
                "kwargs": self.op_kwargs,
            }
            with tf.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                json.dump(args_data, f, default=str)
                args_file = f.name

            try:
                pixi_path = _ensure_pixi_available(self.pixi_binary, auto_install=self.auto_install_pixi)
                cmd = [
                    pixi_path,
                    "run",
                    "--manifest-path",
                    self._manifest_dir,
                ]
                if self.environment:
                    cmd.extend(["--environment", self.environment])
                cmd.extend(["python", "-c", RUNNER_SCRIPT])

                env = os.environ.copy()
                env["AIRFLOW_PIXI_ARGS_FILE"] = args_file

                # Apply cache directories from Airflow Variables if configured
                if self.pixi_cache_dir_variable:
                    path = Variable.get(self.pixi_cache_dir_variable, default_var=None)
                    if path is not None:
                        env["PIXI_CACHE_DIR"] = str(path).strip()
                if self.uv_cache_dir_variable:
                    path = Variable.get(self.uv_cache_dir_variable, default_var=None)
                    if path is not None:
                        env["UV_CACHE_DIR"] = str(path).strip()
                if self.pip_cache_dir_variable:
                    path = Variable.get(self.pip_cache_dir_variable, default_var=None)
                    if path is not None:
                        env["PIP_CACHE_DIR"] = str(path).strip()

                proc = subprocess.run(
                    cmd,
                    cwd=self._manifest_dir,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=3600,
                    check=False,
                )

                if proc.returncode != 0:
                    raise AirflowException(f"Pixi run failed (exit {proc.returncode}): {proc.stderr or proc.stdout}")

                # Last line of stdout is JSON result
                out = (proc.stdout or "").strip()
                if not out:
                    raise AirflowException("Pixi runner produced no output")
                last_line = out.split("\n")[-1]
                try:
                    result = json.loads(last_line)
                except json.JSONDecodeError:
                    result = last_line
                context["ti"].xcom_push(key="return_value", value=result)
                return result
            finally:
                try:
                    os.unlink(args_file)
                except OSError:
                    pass
        finally:
            if temp_dir and self.cleanup_temp_manifest:
                shutil.rmtree(temp_dir, ignore_errors=True)
