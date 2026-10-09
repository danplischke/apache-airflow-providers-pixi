"""Call a function inside the Pixi environment and hand its result back to the worker.

The operators run this module's source as ``python -c <source> <serializer> <input> <output> [json]``.

The input file holds the callable, either ``"module"`` and ``"name"`` or a function's ``"source"``, ``"filename"``
and ``"name"``, with its ``"args"``, ``"kwargs"`` and an optional JSON-safe ``"context"``, in the serializer's
format. The return value goes to the output file, which keeps stdout and stderr free for the task log. With
``json`` as the fourth argument, a pickled result is written as a base64 JSON string, for a pod's XCom file.

An exception from the callable, or an environment Python older than [`MIN_PYTHON`][MIN_PYTHON], is described in
``<output>`` + [`ERROR_SUFFIX`][ERROR_SUFFIX] and, when the environment variable
[`TERMINATION_LOG_ENV`][TERMINATION_LOG_ENV] names a file, there too, shortened to fit a Kubernetes termination message.
Either exits with 1.
"""

from __future__ import annotations

import asyncio
import base64
import importlib
import inspect
import json
import os
import pickle
import sys
import traceback
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

ERROR_SUFFIX = ".error"
"""Appended to the output path for the file describing an exception from the callable."""

TERMINATION_LOG_ENV = "PIXI_AIRFLOW_TERMINATION_LOG"
"""Environment variable naming the file Kubernetes reads as the container's termination message."""

TERMINATION_KEY = "pixi_callable_error"
"""Key of the exception's description in the termination message."""

TERMINATION_MESSAGE_BYTES = 4000
"""The longest termination message written; Kubernetes keeps up to 4 KiB of it."""

PICKLE_PROTOCOL = 4
"""Pickle protocol of the input and the result, readable by every Python the runner supports."""

MIN_PYTHON = (3, 10)
"""The oldest Python the runner supports in the Pixi environment."""

SHIPPED_MODULE = "__pixi_airflow_task__"
"""``__name__`` of the namespace a shipped function's source runs in."""


def unsupported_python(version_info: tuple[int, ...]) -> str | None:
    """Return why the environment's Python cannot run the callable, or ``None`` if it is new enough."""
    if version_info[:2] >= MIN_PYTHON:
        return None
    found = ".".join(str(part) for part in version_info[:3])
    needed = ".".join(str(part) for part in MIN_PYTHON)
    return f"The Pixi environment has Python {found}, but apache-airflow-providers-pixi needs Python {needed} or newer."


def load_spec(serializer: str, path: str) -> dict[str, Any]:
    """Return the callable's description and arguments from the input file."""
    spec: dict[str, Any]
    if serializer == "pickle":
        with open(path, "rb") as f:
            spec = pickle.load(f)
    else:
        with open(path) as f:
            spec = json.load(f)
    return spec


def load_callable(spec: Mapping[str, Any]) -> Callable[..., Any]:
    """Return the callable: the shipped function, compiled under the DAG file's name, or ``module:name``."""
    if "source" in spec:
        namespace: dict[str, Any] = {"__name__": SHIPPED_MODULE}
        exec(compile(spec["source"], spec["filename"], "exec"), namespace)
        function: Callable[..., Any] = namespace[spec["name"]]
        return function
    attribute: Callable[..., Any] = getattr(importlib.import_module(spec["module"]), spec["name"])
    return attribute


def with_context(
    fn: Callable[..., Any], args: Sequence[Any], kwargs: dict[str, Any], context: Mapping[str, Any]
) -> dict[str, Any]:
    """Return ``kwargs`` with the context values for the parameters ``args`` and ``kwargs`` leave open.

    A function with ``**kwargs`` gets every context value, as Airflow's ``determine_kwargs`` passes them.
    """
    try:
        signature = inspect.signature(fn)
        bound = signature.bind_partial(*args, **kwargs).arguments
    except (TypeError, ValueError):
        return kwargs
    taken = set(bound)
    names: list[str] = []
    for name, parameter in signature.parameters.items():
        if parameter.kind == parameter.VAR_KEYWORD:
            taken.discard(name)
            taken.update(bound.get(name, ()))
            names = list(context)
            break
        if parameter.kind in (parameter.POSITIONAL_OR_KEYWORD, parameter.KEYWORD_ONLY):
            names.append(name)
    extra = {name: context[name] for name in names if name in context and name not in taken}
    if not extra:
        return kwargs
    extra.update(kwargs)
    return extra


def await_result(result: Any) -> Any:
    """Return ``result``, awaited first if it is awaitable, as the result of a coroutine function."""
    if not inspect.isawaitable(result):
        return result
    if asyncio.iscoroutine(result):
        return asyncio.run(result)
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(result)
    finally:
        loop.close()


def encode_result(result: Any, serializer: str, as_json: bool) -> str | bytes:
    """Return the content of the output file for ``result``."""
    if serializer == "pickle":
        data = pickle.dumps(result, protocol=PICKLE_PROTOCOL)
        return json.dumps(base64.b64encode(data).decode()) if as_json else data
    try:
        return json.dumps(result)
    except TypeError as e:
        raise TypeError(f"{e}; return a JSON-serializable value or pass serializer='pickle'") from None


def describe_error(error: BaseException) -> dict[str, str]:
    """Return the exception's ``type`` (with its module unless built in), ``message`` and ``traceback``."""
    cls = type(error)
    name = cls.__name__ if "<locals>" in cls.__qualname__ else cls.__qualname__
    if cls.__module__ not in ("builtins", "__main__", SHIPPED_MODULE):
        name = f"{cls.__module__}.{name}"
    try:
        message = str(error)
    except Exception:
        message = "<str() of the exception failed>"
    text = "".join(traceback.format_exception(cls, error, error.__traceback__))
    return {"type": name, "message": message, "traceback": text}


def termination_message(error: Mapping[str, str], limit: int = TERMINATION_MESSAGE_BYTES) -> bytes:
    """Return ``error`` as a termination message of at most ``limit`` bytes, keeping the end of the traceback."""
    shortened = {**error, "message": error["message"][:500]}
    trace = error["traceback"]
    while True:
        data = json.dumps({TERMINATION_KEY: {**shortened, "traceback": trace}}, ensure_ascii=False).encode()
        if len(data) <= limit or not trace:
            return data
        trace = trace[len(trace) // 4 + 1 :]


def report_error(error: Mapping[str, str], output_path: str) -> None:
    """Print the traceback and write the error file, and the termination message when one is asked for."""
    sys.stderr.write(error["traceback"])
    with open(output_path + ERROR_SUFFIX, "w") as f:
        json.dump(error, f)
    termination_log = os.environ.get(TERMINATION_LOG_ENV)
    if termination_log:
        try:
            with open(termination_log, "wb") as f:
                f.write(termination_message(error))
        except OSError:
            pass


def main(argv: Sequence[str]) -> int:
    """Run the callable described by ``argv`` (``-c``, serializer, input, output, optionally ``json``)."""
    serializer, input_path, output_path = argv[1:4]
    problem = unsupported_python(tuple(sys.version_info[:3]))
    if problem:
        report_error(describe_error(RuntimeError(problem)), output_path)
        return 1
    spec = load_spec(serializer, input_path)
    try:
        fn = load_callable(spec)
        kwargs = spec["kwargs"]
        if spec.get("context"):
            kwargs = with_context(fn, spec["args"], kwargs, spec["context"])
        data = encode_result(await_result(fn(*spec["args"], **kwargs)), serializer, list(argv[4:]) == ["json"])
    except Exception as e:
        report_error(describe_error(e), output_path)
        return 1
    with open(output_path, "wb" if isinstance(data, bytes) else "w") as f:
        f.write(data)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
