# System Tests

The example DAGs below run end to end through Airflow's real task runner (`dag.test()`) with a real
`pixi` binary. They are the source of the examples in the guides.

## Example DAGs

```python title="tests/system/pixi/example_pixi.py"
--8<-- "tests/system/pixi/example_pixi.py"
```

`pixi_example` passes an XCom from a regular task into a `@task.pixi` function with its own inline
environment, and its result into a `PixiOperator` that imports a module of an existing project, whose
path is a templated param. `pixi_example_fail` checks that an exception inside the environment fails
the DAG run.

## Running the tests

```bash
uv sync --group dev

# unit tests: a fake pixi runs the command with this Python
pytest tests/unit

# integration tests: real pixi on PATH, needs access to conda-forge
PIXI_INTEGRATION_TEST=1 pytest tests/integration

# system tests: dag.test() with the real task runner and pixi
export AIRFLOW_HOME=/tmp/airflow-e2e AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/tests/system/pixi
airflow db migrate
PIXI_E2E_TEST=1 pytest tests/system/pixi
```
