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
[Choosing the environment](../operators/pixi.md#choosing-the-environment).

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
- **Arguments and the return value are serialized**, as JSON by default or with
  `serializer="pickle"`. See [Arguments and return values](../operators/pixi.md#arguments-and-return-values).

Task decorators such as `@task.pixi`, `@setup`, `@teardown`, `@task.skip_if` and `@task.run_if` are
removed from the shipped source. Line numbers are kept, so a traceback raised inside the environment
points at the right line of the DAG file.
