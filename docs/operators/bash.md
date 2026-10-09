# Pixi Bash Operator

Use the [`PixiBashOperator`][airflow.providers.pixi.operators.bash.PixiBashOperator] to run a Bash
command inside a [Pixi](https://pixi.sh) environment. It works like Airflow's `BashOperator` and
accepts all of its arguments, plus the environment arguments of the
[Pixi Operator](pixi.md#choosing-the-environment) and its environment variable arguments: `env_vars`,
`env_from_variables`, `env_from_connections`, `pixi_conn_id` and the cache directory Variables.

To run a task defined in the manifest's `[tasks]`, use the [Pixi Project Task Operator](project_task.md).

## Using the operator

The command runs as

```text
pixi run --manifest-path <manifest> [--environment <env>] [--locked | --frozen] bash -c '<bash_command>'
```

so every part of it, pipes and `&&` included, runs in the activated environment:

```python
from airflow.providers.pixi.operators.bash import PixiBashOperator

PixiBashOperator(
    task_id="train",
    pixi_project_path="/repo",
    environment="{{ params.env }}",
    bash_command="python train.py --epochs {{ params.epochs }} | tee train.log",
)
```

- **Working directory:** `cwd` defaults to the manifest's directory, so paths relative to the
  project work. Set `cwd` to run somewhere else.
- **Environment:** a predefined one with `pixi_project_path` or `pixi_toml_path` and `environment`,
  or one defined in the DAG with `dependencies` and `pypi_dependencies`. `environment`
  is templated, so it can be chosen per run. `lock_mode` works as for the
  [Pixi Operator](pixi.md#the-lock-file), and relative paths are relative to the DAG file
  ([Relative paths](pixi.md#relative-paths)).
- **Return value:** the last line of output is the task's XCom, as for `BashOperator`
  (`output_processor` and `do_xcom_push` work the same way).
- **Exit codes:** exit code 99 skips the task (`skip_on_exit_code`), any other non-zero code fails it.
- **Templates:** `bash_command` is a Jinja template and may name a `.sh` or `.bash` file, as for
  `BashOperator`.

## Environment variables

The operator takes `BashOperator`'s `env` and `append_env`, and the environment variable arguments of
the [Pixi Operator](pixi.md#environment-variables):

- `env_vars`: variables for the run, not templated, so secrets set here are never rendered into the UI.
- `env_from_variables` and `env_from_connections`: variables from Airflow Variables and Connections,
  read when the task runs ([From Variables and Connections](pixi.md#from-variables-and-connections)).
- `pixi_conn_id`: credentials for private channels and indexes
  ([Private channels and indexes](pixi.md#private-channels-and-indexes)).
- `pixi_cache_dir_variable`, `uv_cache_dir_variable` and `pip_cache_dir_variable`
  ([Cache directories](pixi.md#cache-directories)).

All of them can be set for every task through `default_args`, and apply to `PixiOperator`, `PixiBashOperator`
and the [Pixi Project Task Operator](project_task.md) alike:

```python
with DAG(
    "my_pipeline",
    default_args={
        "pixi_cache_dir_variable": "pixi_cache_dir",
        "env_from_connections": {"DB_PASSWORD": "warehouse.password"},
    },
) as dag:
    PixiBashOperator(
        task_id="load",
        pixi_project_path="/repo",
        bash_command="python load.py",
        env={"BATCH_SIZE": "{{ params.batch_size }}"},
        append_env=True,
    )
```

When several sources set the same variable, the later one in this list wins:

1. the worker's environment, unless `env` is set without `append_env=True`, without the worker's
   Python variables such as `PYTHONPATH` and with `PYTHONNOUSERSITE=1`
   (see [Isolation from the worker's Python](pixi.md#isolation-from-the-workers-python)),
2. the cache directory Variables,
3. `env_from_variables` and `env_from_connections`,
4. `env`,
5. `env_vars`.

Airflow's `AIRFLOW_CTX_*` variables are set as for `BashOperator`.

!!! warning "`env` replaces the worker's environment"
    As for `BashOperator`, `env` replaces the worker's environment unless `append_env=True`. The
    variables of the other sources are still set, but `HOME`, `PATH` and everything else from the worker
    are not. Pixi needs at least `HOME` and `PATH`, so pass `append_env=True` together with `env`.

## `@task.pixi_bash`

As with `@task.bash`, the function runs on the worker and returns the command, which is rendered as a
template and then run in the Pixi environment:

```python
from airflow.sdk import task


@task.pixi_bash(pixi_project_path="/repo")
def train(epochs: int) -> str:
    return f"python train.py --epochs {epochs}"
```

It accepts every `PixiBashOperator` argument.

## Templated fields

`bash_command`, `env`, `cwd`, `pixi_project_path`, `pixi_toml_path`, `environment`, `lock_mode`,
`pypi_dependencies`, `pixi_cache_dir_variable`, `uv_cache_dir_variable` and `pip_cache_dir_variable`.
`env_vars`, `env_from_variables`, `env_from_connections` and `pixi_conn_id` are not templated.

## Reference

For the full list of parameters, see the
[`PixiBashOperator` API reference][airflow.providers.pixi.operators.bash.PixiBashOperator].
