"""Every Pixi operator, sensor and decorator that runs on the worker, for `just standalone`.

Needs pixi 0.81.0 or newer on PATH. The first run solves and installs the environments, which takes a
while; later runs reuse them (dev/project/.pixi, and .airflow/pixi-envs for the inline one).
"""

from __future__ import annotations

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Param, task

from airflow.providers.pixi.operators.bash import PixiBashOperator
from airflow.providers.pixi.operators.pixi import PixiOperator, PixiShortCircuitOperator
from airflow.providers.pixi.operators.task import PixiTaskOperator
from airflow.providers.pixi.sensors.pixi import PixiSensor

PROJECT = "../project"
ENV_CACHE = "../../.airflow/pixi-envs"
MARKER = "/tmp/pixi-showcase-{{ run_id | replace(':', '-') | replace('+', '-') }}"

with DAG(
    "pixi_showcase",
    schedule=None,
    params={"env": Param("default", enum=["default", "lint"], description="environment of dev/project")},
    tags=["pixi"],
):

    @task.pixi(dependencies={"python": "3.12.*", "numpy": ">=2"}, env_cache_path=ENV_CACHE)
    def random_values(count: int, run_id=None) -> list[float]:
        import zlib

        import numpy as np

        return np.random.default_rng(zlib.crc32(run_id.encode())).random(count).round(3).tolist()

    values = random_values(5)

    stats = PixiOperator(
        task_id="summarize",
        pixi_project_path=PROJECT,
        python_callable="jobs:summarize",
        op_args=[values],
        env_from_variables={"SHOWCASE_LABEL": "pixi_showcase_label"},
    )

    tools = PixiBashOperator(
        task_id="tool_versions",
        pixi_project_path=PROJECT,
        environment="{{ params.env }}",
        lock_mode="locked",
        bash_command="python --version && (ruff --version || echo 'no ruff in this environment')",
    )

    @task.pixi_bash(pixi_project_path=PROJECT)
    def report(summary: dict) -> str:
        return f"echo '{summary['label']}: mean of {summary['n']} values: {summary['mean']:.3f}' && which python"

    @task.pixi_sensor(pixi_project_path=PROJECT, poke_interval=5, timeout=300)
    def marker_written(path: str) -> dict:
        import os

        return {"is_done": os.path.exists(path), "xcom_value": path}

    marker_exists = PixiSensor(
        task_id="marker_exists",
        pixi_project_path=PROJECT,
        python_callable="os.path:exists",
        op_args=[MARKER],
        poke_interval=5,
        timeout=300,
    )

    write_marker = PixiBashOperator(
        task_id="write_marker",
        pixi_project_path=PROJECT,
        bash_command=f"sleep 15 && touch {MARKER}",
    )

    describe = PixiTaskOperator(task_id="describe", pixi_project_path=PROJECT, task="describe", task_args=values)

    @task.pixi_branch(pixi_project_path=PROJECT)
    def pick_by_mean(summary: dict) -> str:
        return "high_mean" if summary["mean"] > 0.5 else "low_mean"

    def default_environment(params=None) -> bool:
        return params["env"] == "default"

    only_default = PixiShortCircuitOperator(
        task_id="only_default_environment",
        pixi_project_path=PROJECT,
        python_callable=default_environment,
    )

    report(stats.output) >> tools
    pick_by_mean(stats.output) >> [EmptyOperator(task_id="high_mean"), EmptyOperator(task_id="low_mean")]
    only_default >> describe
    marker_written(MARKER)


with DAG(
    "pixi_showcase_failures",
    schedule=None,
    params={"values": Param([], type="array", description="numbers to average; empty fails the task")},
    tags=["pixi"],
):

    @task.pixi(pixi_project_path=PROJECT)
    def mean(params=None) -> float:
        import statistics

        return statistics.mean(params["values"])

    @task.pixi(pixi_project_path=PROJECT, skip_on_exit_code=3)
    def nothing_to_do() -> None:
        import sys

        sys.exit(3)

    mean()
    nothing_to_do()
