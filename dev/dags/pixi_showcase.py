"""Every Pixi operator and decorator on the worker, for `just standalone`.

Needs pixi 0.81.0 or newer on PATH. The first run solves and installs the environments, which takes a
while; later runs reuse them (dev/project/.pixi, and .airflow/pixi-envs for the inline one).
"""

from __future__ import annotations

from pathlib import Path

from airflow.sdk import DAG, Param, task

from airflow.providers.pixi.operators.bash import PixiBashOperator
from airflow.providers.pixi.operators.pixi import PixiOperator

REPO = Path(__file__).resolve().parents[2]
PROJECT = str(REPO / "dev" / "project")
ENV_CACHE = str(REPO / ".airflow" / "pixi-envs")
MARKER = "/tmp/pixi-showcase-{{ run_id | replace(':', '-') | replace('+', '-') }}"

with DAG(
    "pixi_showcase",
    schedule=None,
    params={"env": Param("default", enum=["default", "lint"], description="environment of dev/project")},
    tags=["pixi"],
):
    # an inline environment, defined here and kept in ENV_CACHE between runs
    @task.pixi(dependencies={"python": "3.12.*", "numpy": ">=2"}, env_cache_path=ENV_CACHE)
    def random_values(count: int) -> list[float]:
        import numpy as np

        return np.random.default_rng().random(count).round(3).tolist()

    # a module of the project, run in its default environment
    stats = PixiOperator(
        task_id="summarize",
        pixi_project_path=PROJECT,
        python_callable="jobs:summarize",
        op_args=[random_values(5)],
    )

    # the environment is picked when the DAG is triggered
    tools = PixiBashOperator(
        task_id="tool_versions",
        pixi_project_path=PROJECT,
        environment="{{ params.env }}",
        bash_command="python --version && (ruff --version || echo 'no ruff in this environment')",
    )

    # the function runs on the worker and returns the command, which runs in the environment
    @task.pixi_bash(pixi_project_path=PROJECT)
    def report(summary: dict) -> str:
        return f"echo 'mean of {summary['n']} values: {summary['mean']:.3f}' && which python"

    # the sensor's function runs in the environment on each poke, until another task writes the marker
    @task.pixi_sensor(pixi_project_path=PROJECT, poke_interval=5, timeout=300)
    def marker_written(path: str) -> dict:
        import os

        return {"is_done": os.path.exists(path), "xcom_value": path}

    write_marker = PixiBashOperator(
        task_id="write_marker",
        pixi_project_path=PROJECT,
        bash_command=f"sleep 15 && touch {MARKER}",
    )

    report(stats.output) >> tools
    marker_written(MARKER)
