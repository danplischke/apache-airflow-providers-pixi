"""What travels into a Pixi environment to call a function there: the runner and the function's source."""

from __future__ import annotations

import ast
import inspect
import textwrap
from collections.abc import Callable, Collection
from importlib.resources import files
from typing import Any

RUNNER_SCRIPT = files("airflow.providers.pixi.runtime").joinpath("runner.py").read_text(encoding="utf-8")
"""Source of :mod:`airflow.providers.pixi.runtime.runner`, which the operators run with ``python -c``."""

STRIPPED_DECORATORS = {"setup", "teardown", "task.skip_if", "task.run_if", "task.pixi", "pixi_task"}


def validate_callable(python_callable: Callable[..., Any] | str) -> None:
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


def function_source(fn: Callable[..., Any], stripped: Collection[str] = STRIPPED_DECORATORS) -> tuple[str, str]:
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
