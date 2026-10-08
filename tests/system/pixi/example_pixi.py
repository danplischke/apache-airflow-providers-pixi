"""Example DAGs for apache-airflow-providers-pixi, run by test_example_dags.py through dag.test()."""

from airflow.sdk import DAG, task

from airflow.providers.pixi.operators.pixi import PixiOperator

ENV = {"dependencies": {"python": "3.12.*"}}

with DAG("pixi_example", params={"project": "", "out": ""}) as dag_ok:

    @task
    def produce() -> int:
        return 20

    # --8<-- [start:task_pixi]
    # Defined in the DAG file, so it reaches the environment as source.
    @task.pixi(**ENV)
    def double(x: int) -> dict:
        import sys

        print("doubling", x)
        return {"value": x * 2, "prefix": sys.prefix}

    # --8<-- [end:task_pixi]

    # --8<-- [start:pixi_operator]
    # A module of the project, whose path is templated; the first argument is double's XCom.
    PixiOperator(
        task_id="report",
        pixi_project_path="{{ params.project }}",
        python_callable="report:write",
        op_args=[double(produce()), "{{ params.out }}"],
    )
    # --8<-- [end:pixi_operator]


with DAG("pixi_example_fail") as dag_fail:

    @task.pixi(**ENV)
    def boom() -> None:
        raise ValueError("task failed on purpose")

    boom()
