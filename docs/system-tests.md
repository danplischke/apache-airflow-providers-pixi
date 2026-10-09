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

```python title="tests/system/pixi/example_pixi_bash_sensor.py"
--8<-- "tests/system/pixi/example_pixi_bash_sensor.py"
```

`pixi_bash_sensor_example` runs `PixiBashOperator`, `@task.pixi_bash` with `lock_mode="locked"`,
`PixiSensor` in poke mode with `lock_mode="frozen"` and `@task.pixi_sensor` in reschedule mode with
`env_cache_path`, and collects their XComs. `pixi_context_example` passes `ds`, `run_id`, `params` and
`logical_date` into a function and reads a Variable through `env_from_variables`.
`pixi_callable_error_example` checks that an exception fails the task with
[`PixiCallableError`][airflow.providers.pixi.exceptions.PixiCallableError] and that
`skip_on_exit_code` skips it.

```python title="tests/system/pixi/example_pixi_kubernetes.py"
--8<-- "tests/system/pixi/example_pixi_kubernetes.py"
```

`pixi_kubernetes_example` runs `@task.pixi_kubernetes` with the JSON and pickle serializers, in sync and
deferrable mode, and checks every XCom in a downstream task. `pixi_kubernetes_example_fail` checks that
an exception in the pod fails the task with its message. These need a Kubernetes cluster; CI runs them
in [kind](https://kind.sigs.k8s.io/) (`.github/workflows/kubernetes.yml`).

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

# Kubernetes system tests: the current kubectl context, for example a kind cluster
PIXI_K8S_E2E_TEST=1 pytest tests/system/pixi/test_example_kubernetes.py
```

`tests/unit/pixi/test_dag_imports.py` loads every DAG file in `dev/dags` and `tests/system/pixi`
through Airflow's `DagBag`, without pixi. A new DAG file has to be added to its `DAG_FILES`.
