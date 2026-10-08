"""Unit tests for PixiSensor and @task.pixi_sensor; a fake pixi runs the callable with this interpreter."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from airflow.sdk import DAG, task
from airflow.sdk.exceptions import AirflowSensorTimeout

from airflow.providers.pixi.sensors.pixi import PixiSensor

INLINE = {"dependencies": {"python": "3.12.*"}}


def ready_on_third_poke(counter: str):
    from pathlib import Path

    path = Path(counter)
    pokes = len(path.read_text()) + 1 if path.exists() else 1
    path.write_text("." * pokes)
    return pokes >= 3


def with_xcom(value):
    return {"is_done": True, "xcom_value": value}


def make(fake_pixi, **kwargs) -> PixiSensor:
    kwargs.setdefault("poke_interval", 0.01)
    return PixiSensor(task_id="s", pixi_binary=str(fake_pixi.path), **INLINE, **kwargs)


def context() -> dict:
    return {"ti": MagicMock(try_number=1), "task_instance": MagicMock(try_number=1)}


def test_pokes_until_the_callable_returns_true(fake_pixi, tmp_path: Path) -> None:
    counter = tmp_path / "pokes"
    make(fake_pixi, python_callable=ready_on_third_poke, op_args=[str(counter)]).execute(context())
    assert counter.read_text() == "..."
    # one inline environment for every poke of the run
    assert len({call["argv"][2] for call in fake_pixi.calls}) == 1
    assert not Path(fake_pixi.calls[-1]["argv"][2]).exists()


def test_poke_reports_the_callables_answer(fake_pixi) -> None:
    assert make(fake_pixi, python_callable="operator:truth", op_args=[0]).poke(context()).is_done is False
    assert make(fake_pixi, python_callable="operator:truth", op_args=[[1]]).poke(context()).is_done is True


def test_is_done_and_xcom_value_become_the_sensors_xcom(fake_pixi) -> None:
    assert make(fake_pixi, python_callable=with_xcom, op_args=[{"rows": 3}]).execute(context()) == {"rows": 3}


def test_a_dict_with_other_keys_is_just_truthy(fake_pixi) -> None:
    result = make(fake_pixi, python_callable="builtins:dict", op_kwargs={"is_done": False, "other": 1}).poke(context())
    assert result.is_done is True
    assert result.xcom_value is None


def test_times_out(fake_pixi) -> None:
    with pytest.raises(AirflowSensorTimeout):
        make(fake_pixi, python_callable="operator:truth", op_args=[0], timeout=0.05).execute(context())


def test_task_pixi_sensor_ships_the_function_without_its_decorator(fake_pixi, tmp_path: Path) -> None:
    with DAG("d") as dag:

        @task.pixi_sensor(poke_interval=0.01, pixi_binary=str(fake_pixi.path), **INLINE)
        def file_exists(path: str) -> dict:
            import os

            return {"is_done": os.path.exists(path), "xcom_value": path}

        file_exists(str(tmp_path))
        file_exists(str(tmp_path))

    op = dag.get_task("file_exists")
    assert type(op).__name__ == "PixiDecoratedSensorOperator"
    assert dag.get_task("file_exists__1")  # unique task ids, as for @task.sensor
    assert "@task.pixi_sensor" not in op.get_python_source()
    assert op.execute(context()) == str(tmp_path)
