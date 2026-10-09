"""Example DAGs for the Bash operator, the sensors, the Airflow context and failing callables.

Run by test_example_bash_sensor.py through dag.test(). ``params.project`` is a pixi project, ``params.out`` a
directory the tasks write their results to.
"""

from __future__ import annotations

import os
import tempfile

from airflow.sdk import DAG, task

from airflow.providers.pixi.operators.bash import PixiBashOperator
from airflow.providers.pixi.operators.pixi import PixiOperator
from airflow.providers.pixi.sensors.pixi import PixiSensor

PARAMS = {"project": "", "out": ""}
ENV_CACHE = os.environ.get("PIXI_E2E_ENV_CACHE") or os.path.join(tempfile.gettempdir(), "airflow-pixi-e2e-envs")


def count_pokes(path: str, pokes: int) -> dict:
    """Record the environment of this poke in ``path``; done on poke number ``pokes``."""
    import sys

    with open(path, "a") as f:
        f.write(sys.prefix + "\n")
    with open(path) as f:
        done = len(f.read().splitlines())
    return {"is_done": done >= pokes, "xcom_value": done}


def context_values(label: str, ds=None, run_id=None, params=None, logical_date=None) -> dict:
    import json
    import os

    values = {"label": label, "ds": ds, "run_id": run_id, "params": params, "logical_date": logical_date}
    values["greeting"] = os.environ.get("GREETING")
    with open(os.path.join(params["out"], "context.json"), "w") as f:
        json.dump(values, f)
    return values


def fail_on_purpose(path: str) -> None:
    raise RuntimeError(f"{path} is empty")


def exit_with_99() -> None:
    import sys

    sys.exit(99)


with DAG("pixi_bash_sensor_example", params=PARAMS) as dag_bash_sensor:
    bash = PixiBashOperator(
        task_id="bash",
        pixi_project_path="{{ params.project }}",
        env={"GREETING": "hello"},
        append_env=True,
        bash_command="python -c 'import os, sys; print(os.environ[\"GREETING\"], sys.prefix)'",
    )

    @task.pixi_bash(pixi_project_path="{{ params.project }}", lock_mode="locked")
    def bash_from_function(word: str) -> str:
        return f"echo \"{word} {{{{ ds }}}} $(python -c 'import sys; print(sys.prefix)')\""

    poke = PixiSensor(
        task_id="poke",
        pixi_project_path="{{ params.project }}",
        lock_mode="frozen",
        python_callable=count_pokes,
        op_args=["{{ params.out }}/poke.txt", 2],
        mode="poke",
        poke_interval=1,
        timeout=600,
    )

    @task.pixi_sensor(
        dependencies={"python": "3.12.*"}, env_cache_path=ENV_CACHE, mode="reschedule", poke_interval=1, timeout=900
    )
    def reschedule(path: str, pokes: int) -> dict:
        import sys

        with open(path, "a") as f:
            f.write(sys.prefix + "\n")
        with open(path) as f:
            done = len(f.read().splitlines())
        return {"is_done": done >= pokes, "xcom_value": done}

    @task
    def collect(bash_line: str, function_line: str, pokes: int, reschedules: int, out: str) -> None:
        import json

        values = {"bash": bash_line, "bash_from_function": function_line, "poke": pokes, "reschedule": reschedules}
        with open(os.path.join(out, "collected.json"), "w") as f:
            json.dump(values, f)

    from_function = bash_from_function("rendered")
    rescheduled = reschedule("{{ params.out }}/reschedule.txt", 2)
    bash >> [from_function, poke]
    collect(bash.output, from_function, poke.output, rescheduled, "{{ params.out }}")


with DAG("pixi_context_example", params=PARAMS) as dag_context:
    PixiOperator(
        task_id="context_values",
        pixi_project_path="{{ params.project }}",
        python_callable=context_values,
        op_args=["from op_args"],
        env_from_variables={"GREETING": "pixi_e2e_greeting"},
    )


with DAG("pixi_callable_error_example", params=PARAMS) as dag_callable_error:
    PixiOperator(
        task_id="raises",
        pixi_project_path="{{ params.project }}",
        python_callable=fail_on_purpose,
        op_args=["input.csv"],
    )

    PixiOperator(
        task_id="skips",
        pixi_project_path="{{ params.project }}",
        python_callable=exit_with_99,
        skip_on_exit_code=99,
    )
