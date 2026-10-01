from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

from airflow.sdk import dag, task


def add_one(x: int) -> int:
    return x + 1


def test_task_decorator_registered() -> None:
    assert hasattr(task, "pixi")


def test_task_pixi_builds_operator() -> None:
    @dag
    def test_dag():
        task.pixi(pixi_project_path="/proj", environment="cuda")(add_one)(2)

    op = test_dag().get_task("add_one")
    assert type(op).__name__ == "PixiDecoratedOperator"
    assert op.custom_operator_name == "@task.pixi"
    assert op.python_callable is add_one
    assert op._callable_ref.endswith(":add_one")
    assert op._manifest_dir == os.path.abspath("/proj")
    assert op.environment == "cuda"
    assert list(op.op_args) == [2]
    assert {"op_args", "op_kwargs", "pixi_project_path"} <= set(op.template_fields)


def test_task_pixi_keeps_xcom_args_and_dependencies() -> None:
    """Regression: PixiOperator must not reset op_args set by the TaskFlow decorator."""

    @dag
    def test_dag():
        @task
        def produce():
            return 1

        task.pixi(pixi_project_path="/proj")(add_one)(produce())

    consume_op = test_dag().get_task("add_one")
    assert consume_op.upstream_task_ids == {"produce"}
    assert len(consume_op.op_args) == 1


def test_task_pixi_execute_calls_function_by_module_path() -> None:
    @dag
    def test_dag():
        task.pixi(pixi_project_path="/proj", auto_install_pixi=False)(add_one)(2)

    op = test_dag().get_task("add_one")
    shipped = {}

    def fake_run(cmd, **kwargs):
        with open(kwargs["env"]["AIRFLOW_PIXI_ARGS_FILE"]) as f:
            shipped.update(json.load(f))
        return MagicMock(returncode=0, stdout="3", stderr="")

    with (
        patch("pixi_airflow.operators.pixi.shutil.which", return_value="/usr/bin/pixi"),
        patch("pixi_airflow.operators.pixi.subprocess.run", side_effect=fake_run) as m_run,
    ):
        assert op.execute({"ti": MagicMock()}) == 3
    assert m_run.call_args[0][0][:4] == ["/usr/bin/pixi", "run", "--manifest-path", os.path.abspath("/proj")]
    assert shipped["callable"] == "add_one"
    assert shipped["module"] == op._callable_ref.split(":")[0]
    assert shipped["args"] == [2]
