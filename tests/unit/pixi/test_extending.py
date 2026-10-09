"""Building on PixiOperator and @task.pixi, as other providers do.

The wrappers here follow lamindb-airflow's ``@task.lamindb_venv``: a mixin rewrites the shipped source so a
runtime wraps the user function, and sets ``env_vars`` and ``requirements`` while executing.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from airflow.sdk import DAG, task
from airflow.sdk.bases.decorator import task_decorator_factory

from airflow.providers.pixi.decorators.pixi import PixiDecoratedOperator
from airflow.providers.pixi.operators.pixi import PixiOperator
from airflow.providers.pixi.utils.compat import AirflowException

if sys.version_info >= (3, 11):
    import tomllib as tomli
else:
    tomli = pytest.importorskip("tomli")

INLINE = {"dependencies": {"python": "3.12.*"}}

RUNTIME = """

def _run_wrapped(fn, args, kwargs, label):
    import os

    return {"label": label, "result": fn(*args, **kwargs), "token": os.environ.get("WRAPPER_TOKEN")}
"""


class WrappingMixin:
    """Run the user function through ``RUNTIME``, with a secret and a package only while executing."""

    _wrapper_config: dict[str, Any] | None = None

    def __init__(self, *, tag: str = "wrapped", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.tag = tag

    def execute(self, context: Any) -> Any:
        env_vars, requirements = self.env_vars, self.requirements
        self._wrapper_config = {"label": self.tag}
        self.env_vars = {"WRAPPER_TOKEN": "secret", **(env_vars or {})}
        if self.inline_manifest:
            self.requirements = [*requirements, "wrapper-runtime>=1"]
        try:
            return super().execute(context)
        finally:
            self.env_vars, self.requirements, self._wrapper_config = env_vars, requirements, None

    def get_python_source(self) -> str:
        source = super().get_python_source()
        if self._wrapper_config is None:
            return source
        name = self.python_callable.__name__
        return (
            source
            + RUNTIME
            + f"\n_user_fn = {name}\n\n\ndef {name}(*args, **kwargs):\n"
            + f"    return _run_wrapped(_user_fn, args, kwargs, **{self._wrapper_config!r})\n"
        )


class WrappedPixiDecoratedOperator(WrappingMixin, PixiDecoratedOperator):
    custom_operator_name = "@task.wrapped_pixi"

    def __init__(self, *, tag: str = "wrapped", **kwargs: Any) -> None:
        super().__init__(tag=tag, **kwargs)


def wrapped_pixi_task(python_callable=None, multiple_outputs=None, **kwargs):
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=WrappedPixiDecoratedOperator,
        **kwargs,
    )


def flow_task_placeholder(config: dict[str, Any]) -> str:
    """Placeholder callable; the operator ships generated source under this name."""
    raise NotImplementedError


class RuntimeFunctionOperator(PixiOperator):
    """Run a function of a runtime shipped as source, with arguments computed at execute time."""

    def __init__(self, *, reference: str, **kwargs: Any) -> None:
        super().__init__(python_callable=flow_task_placeholder, **kwargs)
        self.reference = reference

    def get_python_source(self) -> str:
        return (
            RUNTIME
            + f"\n\ndef {self.python_callable.__name__}(config):\n"
            + "    return _run_wrapped(str.upper, [config['reference']], {}, label='flow')\n"
        )

    def execute(self, context: Any) -> Any:
        self.op_kwargs = {"config": {"reference": f"{self.reference}/{context['run_id']}"}}
        return super().execute(context)


def manifest_of(fake_pixi) -> str:
    return Path(fake_pixi.calls[-1]["argv"][2]).read_text()


def test_mixin_wraps_a_task_pixi_function(fake_pixi, monkeypatch) -> None:
    monkeypatch.setattr(task, "wrapped_pixi", wrapped_pixi_task, raising=False)
    with DAG("wrapped") as dag:

        @task.wrapped_pixi(
            tag="step",
            pypi_dependencies={"pandas": "*"},
            pixi_binary=str(fake_pixi.path),
            cleanup_temp_manifest=False,
        )
        def double(x: int) -> int:
            return x * 2

        double(21)

    op = dag.get_task("double")
    assert op.execute({"ti": MagicMock()}) == {"label": "step", "result": 42, "token": "secret"}
    assert tomli.loads(manifest_of(fake_pixi))["pypi-dependencies"] == {"pandas": "*", "wrapper-runtime": ">=1"}
    assert op.env_vars is None
    assert op.requirements == []


def test_mixin_gets_the_source_of_pixi_operator_through_super(monkeypatch) -> None:
    monkeypatch.setattr(task, "wrapped_pixi", wrapped_pixi_task, raising=False)
    with DAG("wrapped") as dag:

        @task.wrapped_pixi(**INLINE)
        def triple(x: int) -> int:
            return x * 3

        triple(1)

    op = dag.get_task("triple")
    op._wrapper_config = {"label": "step"}
    lines = op.get_python_source().splitlines()
    assert not any("@task.wrapped_pixi" in line for line in lines)
    start = lines.index("def triple(x: int) -> int:")
    assert Path(__file__).read_text().splitlines()[start].strip() == lines[start]
    assert "_run_wrapped" in lines[-1]


def test_unwrapped_source_is_unchanged_outside_execute() -> None:
    with DAG("wrapped") as dag:

        @wrapped_pixi_task(**INLINE)
        def double(x: int) -> int:
            return x * 2

        double(21)

    source = dag.get_task("double").get_python_source()
    assert "def double" in source
    assert "_run_wrapped" not in source


def test_operator_ships_generated_source_for_a_placeholder(fake_pixi) -> None:
    op = RuntimeFunctionOperator(task_id="flow", reference="dag", pixi_binary=str(fake_pixi.path), **INLINE)
    assert op.execute({"ti": MagicMock(), "run_id": "r1"}) == {"label": "flow", "result": "DAG/R1", "token": None}


def test_wrapper_requirements_cannot_extend_a_project_environment(fake_pixi, tmp_path: Path) -> None:
    op = PixiOperator(
        task_id="t",
        pixi_project_path=str(tmp_path),
        python_callable="json:dumps",
        pixi_binary=str(fake_pixi.path),
    )
    assert not op.inline_manifest
    op.requirements = ["wrapper-runtime"]
    with pytest.raises(AirflowException, match=f"can only extend an inline manifest.*{re.escape(str(tmp_path))}"):
        op.execute({"ti": MagicMock()})
    assert fake_pixi.calls == []
