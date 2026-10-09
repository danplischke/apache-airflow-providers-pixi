"""What travels into a Pixi environment to call a function there: the runner and the function's source."""

from __future__ import annotations

import ast
import inspect
import textwrap
from collections.abc import Callable, Collection
from typing import Any

RUNNER_SCRIPT = """
import importlib, inspect, json, pickle, sys


def with_context(fn, args, kwargs, context):
    try:
        signature = inspect.signature(fn)
        bound = signature.bind_partial(*args, **kwargs).arguments
    except (TypeError, ValueError):
        return kwargs
    taken = set(bound)
    names = []
    for name, parameter in signature.parameters.items():
        if parameter.kind == parameter.VAR_KEYWORD:
            taken.discard(name)
            taken.update(bound.get(name, ()))
            names = list(context)
            break
        if parameter.kind in (parameter.POSITIONAL_OR_KEYWORD, parameter.KEYWORD_ONLY):
            names.append(name)
    extra = dict((name, context[name]) for name in names if name in context and name not in taken)
    if not extra:
        return kwargs
    extra.update(kwargs)
    return extra


serializer, input_path, output_path = sys.argv[1:4]
if serializer == "pickle":
    with open(input_path, "rb") as f:
        spec = pickle.load(f)
else:
    with open(input_path) as f:
        spec = json.load(f)
try:
    if "source" in spec:
        namespace = {"__name__": "__pixi_airflow_task__"}
        exec(compile(spec["source"], spec["filename"], "exec"), namespace)
        fn = namespace[spec["name"]]
    else:
        fn = getattr(importlib.import_module(spec["module"]), spec["name"])
    kwargs = spec["kwargs"]
    if spec.get("context"):
        kwargs = with_context(fn, spec["args"], kwargs, spec["context"])
    result = fn(*spec["args"], **kwargs)
    if getattr(inspect, "isawaitable", lambda value: False)(result):
        import asyncio

        if hasattr(asyncio, "run") and asyncio.iscoroutine(result):
            result = asyncio.run(result)
        else:
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(result)
            finally:
                loop.close()
    if serializer == "pickle" and sys.argv[4:] == ["json"]:
        import base64

        data = json.dumps(base64.b64encode(pickle.dumps(result, protocol=4)).decode())
    elif serializer == "pickle":
        data = pickle.dumps(result, protocol=4)
    else:
        try:
            data = json.dumps(result)
        except TypeError as e:
            raise TypeError(str(e) + "; return a JSON-serializable value or pass serializer='pickle'") from None
except Exception as e:
    import traceback

    text = traceback.format_exc()
    sys.stderr.write(text)
    cls = type(e)
    name = cls.__name__ if "<locals>" in cls.__qualname__ else cls.__qualname__
    if cls.__module__ not in ("builtins", "__main__", "__pixi_airflow_task__"):
        name = cls.__module__ + "." + name
    try:
        message = str(e)
    except Exception:
        message = "<str() of the exception failed>"
    with open(output_path + ".error", "w") as f:
        json.dump({"type": name, "message": message, "traceback": text}, f)
    sys.exit(1)
with open(output_path, "wb" if isinstance(data, bytes) else "w") as f:
    f.write(data)
"""

ERROR_SUFFIX = ".error"
"""Appended to the runner's output path for the file describing an exception from the callable."""

STRIPPED_DECORATORS = {"setup", "teardown", "task.skip_if", "task.run_if", "task.pixi", "pixi_task"}

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
