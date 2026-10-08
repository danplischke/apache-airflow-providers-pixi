# Pixi Bash Operator

Use the [`PixiBashOperator`][airflow.providers.pixi.operators.bash.PixiBashOperator] to run a Bash
command inside a [Pixi](https://pixi.sh) environment. It works like Airflow's `BashOperator` and
accepts all of its arguments, plus the environment arguments of the
[Pixi Operator](pixi.md#choosing-the-environment).

## Using the operator

The command runs as

```text
pixi run --manifest-path <manifest> [--environment <env>] bash -c '<bash_command>'
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
  or one defined in the DAG with `dependencies`, `pypi_dependencies` and `requirements`. `environment`
  is templated, so it can be chosen per run.
- **Return value:** the last line of output is the task's XCom, as for `BashOperator`
  (`output_processor` and `do_xcom_push` work the same way).
- **Exit codes:** exit code 99 skips the task (`skip_on_exit_code`), any other non-zero code fails it.
- **Templates:** `bash_command` is a Jinja template and may name a `.sh` or `.bash` file, as for
  `BashOperator`.

!!! warning "`env` replaces the worker's environment"
    As for `BashOperator`, `env` replaces the worker's environment unless `append_env=True`. Pixi
    needs at least `HOME` and `PATH`, so pass `append_env=True` together with `env`.

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

`bash_command`, `env`, `cwd`, `pixi_project_path`, `pixi_toml_path`, `environment` and
`requirements`.
