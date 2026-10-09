# Pixi Task Decorator

Python callables decorated with `@task.pixi` run inside a [Pixi](https://pixi.sh) environment.
The decorator is registered on Airflow's `task` object once the provider is installed.

## Parameters

`@task.pixi` accepts every [`PixiOperator`](../operators/pixi.md) argument, plus:

`multiple_outputs`
:   If set, a returned dict is unrolled to multiple XCom values, with the keys as XCom keys.
    Defaults to `False`.

The environment is chosen the same way as for the operator: exactly one of `pixi_project_path`,
`pixi_toml_path`, or an inline manifest (`dependencies` / `pypi_dependencies`). See
[Choosing the environment](../operators/pixi.md#choosing-the-environment). A relative project path is
relative to the DAG file, and `lock_mode="locked"` installs a project from its `pixi.lock` without
updating it.

## Usage example

```python title="tests/system/pixi/example_pixi.py"
--8<-- "tests/system/pixi/example_pixi.py:task_pixi"
```

With an existing project and one of its environments:

```python
from airflow.sdk import dag, task


@dag
def my_dag():
    @task
    def epochs() -> int:
        return 3

    @task.pixi(pixi_project_path="/path/to/pixi/project", environment="cuda")
    def train(epochs: int) -> float:
        import torch

        return torch.rand(epochs).mean().item()

    train(epochs())


my_dag()
```

## Writing the function

Like `@task.virtualenv`, the function's source is shipped to the environment and run there, so:

- **Imports go inside the function.** The environment does not have the DAG file's imports, and may
  not have Airflow installed at all.
- **No variables from enclosing functions.** A function that uses one is rejected when the DAG is
  parsed; pass the value as an argument instead.
- **`async def` works too.** The environment awaits the coroutine with `asyncio.run`; the event
  loop exists only for that call.
- **Arguments and the return value are serialized**, as JSON by default or with
  `serializer="pickle"`. See [Arguments and return values](../operators/pixi.md#arguments-and-return-values).
- **Context values arrive as parameters**, as for `@task.virtualenv`: `ds`, `params`, `run_id`,
  `logical_date` and the other keys listed in [Airflow context](../operators/pixi.md#airflow-context),
  as JSON values. Dates are ISO 8601 strings. Give these parameters a default, as for any TaskFlow
  function:

    ```python
    @task.pixi(pypi_dependencies=["pandas", "pyarrow"])
    def report(table: str, ds=None, params=None) -> int:
        import pandas as pd

        return len(pd.read_parquet(f"{params['root']}/{table}/{ds}.parquet"))
    ```

    `ti`, `dag_run` and other Airflow objects don't reach the environment, and
    `get_current_context()` isn't available there.

- **An exception fails the task with
  [`PixiCallableError`][airflow.providers.pixi.exceptions.PixiCallableError]**, such as
  `train raised ValueError: no rows`, after its traceback is printed to the task log. To skip the task
  instead, exit with a code listed in `skip_on_exit_code`, for example `sys.exit(99)`. See
  [Errors, skipping and timeouts](../operators/pixi.md#errors-skipping-and-timeouts).

Task decorators such as `@task.pixi`, `@setup`, `@teardown`, `@task.skip_if` and `@task.run_if` are
removed from the shipped source, and so is the decorator of a provider that builds on `@task.pixi`
(see [Building on the Pixi Operator](../extending.md)). Line numbers are kept, so a traceback raised
inside the environment points at the right line of the DAG file.
