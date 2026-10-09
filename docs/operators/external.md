# Pixi External Python Operator

Use the
[`PixiExternalPythonOperator`][airflow.providers.pixi.operators.external.PixiExternalPythonOperator]
to run a Python callable with the Python of a [Pixi](https://pixi.sh) environment that is already
installed. It is the Pixi counterpart of `ExternalPythonOperator`. It skips `pixi run`, so no pixi is
needed when the task runs, and nothing is solved, downloaded or installed.

This suits worker images and hosts whose environments are installed with `pixi install` when they are
built. Where pixi is available at run time, the [Pixi Operator](pixi.md) is usually the better choice:
it installs a missing or outdated environment and applies the environment's activation.

## Using the operator

Install the environment when the image is built:

```dockerfile
FROM apache/airflow:3.3.2-python3.12
COPY --from=ghcr.io/prefix-dev/pixi:0.81.0 /usr/local/bin/pixi /usr/local/bin/pixi

COPY --chown=airflow pipelines /opt/pipelines
RUN pixi install --manifest-path /opt/pipelines --frozen
```

Then run the callable in it:

```python
from airflow.providers.pixi.operators.external import PixiExternalPythonOperator


def summarize(path: str) -> dict:
    import pandas as pd

    return pd.read_parquet(path).describe().to_dict()


PixiExternalPythonOperator(
    task_id="summarize",
    pixi_project_path="/opt/pipelines",
    python_callable=summarize,
    op_args=["/data/input.parquet"],
)
```

The callable runs with `<workspace>/.pixi/envs/<environment>/bin/python`. The workspace is
`pixi_project_path`, or the directory of `pixi_toml_path`. `environment` chooses the environment and
defaults to `default`. The run's working directory is the workspace, so modules next to the manifest
can be imported, and `python_callable="jobs:train"` works as for the Pixi Operator.

Everything about the callable works as for the [Pixi Operator](pixi.md#using-the-operator): source
shipping, `op_args` and `op_kwargs`, `serializer`, the [Airflow context](pixi.md#airflow-context),
return values and [errors](pixi.md#errors-skipping-and-timeouts). `env_vars`, `env_from_variables`
and `env_from_connections` set
[environment variables](pixi.md#from-variables-and-connections) as there. As
there, the worker's `PYTHONPATH`, `PYTHONHOME`, `PYTHONUSERBASE` and `VIRTUAL_ENV` are left out.
`lock_mode`, `pixi_binary`, `pixi_conn_id` and the cache directory Variables have no effect, so
values for them in `default_args` don't get in the way.

## What differs from `pixi run`

- **No activation.** `PATH` starts with the environment's `bin` directory and `CONDA_PREFIX` points
  at the environment. The activation scripts of its packages and the manifest's `[activation]` are
  not applied. Set the variables they would set with `env_vars`.
- **No install.** An environment that is missing fails the task, with the `pixi install` command
  that installs it. An outdated environment runs as it is, so install with `--frozen` or `--locked`
  when the image is built.
- **No inline manifests**, which are not installed anywhere. Use `pixi_project_path` or
  `pixi_toml_path`.
- **No detached environments.** Pixi's `detached-environments` setting installs environments outside
  the workspace, where the operator does not look.

## `@task.pixi_external`

```python
from airflow.sdk import task


@task.pixi_external(pixi_project_path="/opt/pipelines", environment="cuda")
def train(epochs: int, ds=None) -> float:
    import torch

    ...
```

It accepts every `PixiExternalPythonOperator` argument. As for `@task.pixi`, the function must be
self-contained; see [Writing the function](../decorators/pixi.md#writing-the-function).
