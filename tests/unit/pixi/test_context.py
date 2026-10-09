"""The Airflow context a callable in the Pixi environment receives; a fake pixi runs it with this interpreter."""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pendulum
import pytest
from airflow.sdk import DAG, Param, task
from airflow.sdk.definitions.param import ParamsDict

from airflow.providers.pixi.operators.pixi import PixiOperator
from airflow.providers.pixi.utils.context import CONTEXT_KEYS, serializable_context

INLINE = {"dependencies": {"python": "3.12.*"}}
LOGICAL_DATE = pendulum.datetime(2026, 1, 2, 3, 4, 5, tz="UTC")


def task_context(**overrides) -> dict:
    """A context as the task runner builds it, with the values JSON cannot hold that it also has."""
    ti = SimpleNamespace(dag_id="pipeline", task_id="step", map_index=-1, try_number=2)
    context = {
        "ti": ti,
        "task_instance": ti,
        "run_id": "manual__2026-01-02",
        "ds": "2026-01-02",
        "ds_nodash": "20260102",
        "ts": LOGICAL_DATE.isoformat(),
        "logical_date": LOGICAL_DATE,
        "data_interval_start": LOGICAL_DATE,
        "data_interval_end": LOGICAL_DATE.add(days=1),
        "prev_start_date_success": None,
        "params": ParamsDict({"rows": Param(10, type="integer"), "label": "all"}),
        "dag_run": SimpleNamespace(conf={"source": "s3://bucket", "when": datetime.date(2026, 1, 1)}),
        "macros": MagicMock(),
        "var": MagicMock(),
        "task": MagicMock(),
        "dag": MagicMock(),
    }
    context.update(overrides)
    return context


def make(fake_pixi, **kwargs) -> PixiOperator:
    if "pixi_project_path" not in kwargs:
        kwargs.update(INLINE)
    return PixiOperator(task_id="step", pixi_binary=str(fake_pixi.path), **kwargs)


def test_serializable_context_holds_json_values() -> None:
    context = serializable_context(task_context())
    assert context == {
        "run_id": "manual__2026-01-02",
        "ds": "2026-01-02",
        "ds_nodash": "20260102",
        "ts": "2026-01-02T03:04:05+00:00",
        "logical_date": "2026-01-02T03:04:05+00:00",
        "data_interval_start": "2026-01-02T03:04:05+00:00",
        "data_interval_end": "2026-01-03T03:04:05+00:00",
        "prev_start_date_success": None,
        "dag_id": "pipeline",
        "task_id": "step",
        "map_index": -1,
        "try_number": 2,
        "params": {"rows": 10, "label": "all"},
        "conf": {"source": "s3://bucket", "when": "2026-01-01"},
    }
    assert json.loads(json.dumps(context)) == context
    assert set(context) <= set(CONTEXT_KEYS)


def test_values_json_cannot_hold_are_left_out() -> None:
    context = serializable_context(
        task_context(
            ti=MagicMock(),
            run_id=object(),
            params={"ok": [1, "a"], "client": object(), 3: "not a str key"},
            dag_run=SimpleNamespace(conf={"ok": True, "fn": print}),
        )
    )
    assert "run_id" not in context
    assert "dag_id" not in context
    assert context["params"] == {"ok": [1, "a"]}
    assert context["conf"] == {"ok": True}


def test_dag_and_task_ids_without_a_task_instance() -> None:
    context = serializable_context(
        {"dag": SimpleNamespace(dag_id="pipeline"), "task": SimpleNamespace(task_id="step"), "params": {}}
    )
    assert context == {"dag_id": "pipeline", "task_id": "step", "params": {}}


def test_no_context_is_empty() -> None:
    assert serializable_context(None) == {}
    assert serializable_context({}) == {}


def test_callable_input_has_the_context_only_when_given(fake_pixi) -> None:
    op = make(fake_pixi, python_callable="json:dumps")
    assert "context" not in json.loads(op.callable_input())
    assert json.loads(op.callable_input(task_context()))["context"]["ds"] == "2026-01-02"


def context_values(x, ds, params, run_id, logical_date, conf=None, dag_id=None, try_number=None):
    return [x, ds, params, run_id, logical_date, conf, dag_id, try_number]


def test_parameters_named_after_context_keys_get_their_values(fake_pixi) -> None:
    result = make(fake_pixi, python_callable=context_values, op_args=[1]).execute(task_context())
    assert result == [
        1,
        "2026-01-02",
        {"rows": 10, "label": "all"},
        "manual__2026-01-02",
        "2026-01-02T03:04:05+00:00",
        {"source": "s3://bucket", "when": "2026-01-01"},
        "pipeline",
        2,
    ]


def ds_and_run_id(ds, run_id="default"):
    return [ds, run_id]


@pytest.mark.parametrize(
    ("op_args", "op_kwargs", "expected"),
    [
        (["positional"], {}, ["positional", "manual__2026-01-02"]),
        ([], {"ds": "keyword"}, ["keyword", "manual__2026-01-02"]),
        ([], {"run_id": None}, ["2026-01-02", None]),
    ],
)
def test_op_args_and_op_kwargs_win_over_the_context(fake_pixi, op_args, op_kwargs, expected) -> None:
    op = make(fake_pixi, python_callable=ds_and_run_id, op_args=op_args, op_kwargs=op_kwargs)
    assert op.execute(task_context()) == expected


def var_keyword(x, **context):
    return {"x": x, "keys": sorted(context), "ds": context["ds"], "run_id": context["run_id"]}


def test_var_keyword_gets_the_whole_context(fake_pixi) -> None:
    op = make(fake_pixi, python_callable=var_keyword, op_kwargs={"x": 1, "run_id": "mine"})
    result = op.execute(task_context())
    assert result["x"] == 1
    assert result["ds"] == "2026-01-02"
    assert result["run_id"] == "mine"
    assert result["keys"] == sorted(serializable_context(task_context()))


def positional_only(ds, /):
    return ds


def test_positional_only_parameters_are_not_filled(fake_pixi) -> None:
    with pytest.raises(Exception, match="missing 1 required positional argument: 'ds'"):
        make(fake_pixi, python_callable=positional_only).execute(task_context())


def test_a_module_path_callable_gets_the_context_too(fake_pixi, tmp_path: Path) -> None:
    (tmp_path / "pixi.toml").write_text("")
    (tmp_path / "jobs.py").write_text("def report(table, ds, params=None):\n    return [table, ds, params['rows']]\n")
    op = make(fake_pixi, pixi_project_path=str(tmp_path), python_callable="jobs:report", op_args=["sales"])
    assert op.execute(task_context()) == ["sales", "2026-01-02", 10]


def test_pickle_serializer_passes_the_context(fake_pixi) -> None:
    op = make(fake_pixi, python_callable=ds_and_run_id, serializer="pickle")
    assert op.execute(task_context()) == ["2026-01-02", "manual__2026-01-02"]


def test_task_pixi_fills_context_parameters(fake_pixi) -> None:
    with DAG("pipeline") as dag:

        @task.pixi(pixi_binary=str(fake_pixi.path), **INLINE)
        def step(x: int, ds=None, params=None, ti=None):
            return [x, ds, params, ti]

        step(1)

    assert dag.get_task("step").execute(task_context()) == [1, "2026-01-02", {"rows": 10, "label": "all"}, None]


def test_without_a_context_the_parameters_keep_their_defaults(fake_pixi) -> None:
    assert make(fake_pixi, python_callable=ds_and_run_id, op_args=["x"]).execute({"ti": MagicMock()}) == [
        "x",
        "default",
    ]


def user_step(x, ds=None):
    return [x, ds]


class WrappingOperator(PixiOperator):
    """Ships a wrapper around the function, as in docs/extending.md."""

    def get_python_source(self) -> str:
        return (
            super().get_python_source()
            + "\nimport functools\n_user = user_step\n\n\n@functools.wraps(_user)\n"
            + "def user_step(*args, **kwargs):\n    return {'wrapped': _user(*args, **kwargs)}\n"
        )


def test_a_wrapper_with_functools_wraps_gets_the_parameters_of_the_function(fake_pixi) -> None:
    op = WrappingOperator(
        task_id="step", python_callable=user_step, op_args=[1], pixi_binary=str(fake_pixi.path), **INLINE
    )
    assert op.execute(task_context()) == {"wrapped": [1, "2026-01-02"]}
