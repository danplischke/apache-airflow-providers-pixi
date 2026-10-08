"""What travels into a Pixi environment to call a function there: the runner and the function's source."""

from __future__ import annotations

import ast
import inspect
import textwrap
from collections.abc import Callable, Collection
from typing import Any

# Runs inside the Pixi environment as `python -c RUNNER_SCRIPT <serializer> <input> <output> [json]`. The input
# file holds the callable (a module path or a function's source) and its arguments; the return value goes
# to the output file, which keeps stdout and stderr free for the task log. With "json", a pickled result is
# written as a base64 JSON string, for a pod's XCom file. Must run on old Pythons too.
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
if serializer == "pickle" and sys.argv[4:] == ["json"]:
    import base64

    with open(output_path, "w") as f:
        json.dump(base64.b64encode(pickle.dumps(result, protocol=4)).decode(), f)
elif serializer == "pickle":
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
STRIPPED_DECORATORS = {"setup", "teardown", "task.skip_if", "task.run_if", "task.pixi", "pixi_task"}

# pickle protocol readable by every Python 3.4+, since the environment may run an older Python
PICKLE_PROTOCOL = 4


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
