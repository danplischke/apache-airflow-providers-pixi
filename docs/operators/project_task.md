# Pixi Project Task Operator

Use the [`PixiProjectTaskOperator`][airflow.providers.pixi.operators.project_task.PixiProjectTaskOperator] to run a task
defined in the `[tasks]` of a [Pixi](https://pixi.sh) manifest, as `pixi run <task>` does on the
command line. It is built on the [Pixi Bash Operator](bash.md) and accepts its arguments, except
`bash_command`.

## Using the operator

Given a project with tasks:

```toml
# /repo/pixi.toml
[tasks]
train = "python train.py"

[tasks.report]
args = ["day", { arg = "format", default = "html" }]
cmd = "python report.py --day {{ day }} --format {{ format }}"
```

run one of them with `task`, and pass its arguments with `task_args`:

```python
from airflow.providers.pixi.operators.project_task import PixiProjectTaskOperator

PixiProjectTaskOperator(
    task_id="report",
    pixi_project_path="/repo",
    lock_mode="locked",
    task="report",
    task_args=["{{ ds }}", "pdf"],
)
```

The command is

```text
pixi run --manifest-path <manifest> [--environment <env>] [--locked | --frozen] <task> <task_args...>
```

It runs through `BashOperator`'s `bash -c`, with each argument shell-quoted, so each argument reaches
pixi unchanged: bash does not split it at spaces or expand `$` in it. What pixi does with an argument
then depends on the task, see [Task arguments](#task-arguments).

- **Environment:** `pixi_project_path` or `pixi_toml_path`, with `environment` and `lock_mode`, as
  for the [Pixi Operator](pixi.md#choosing-the-environment). An inline manifest
  (`dependencies`, `pypi_dependencies`) cannot define tasks and is rejected when the
  DAG is parsed.
- **Return value:** the last line of output is the task's XCom (`output_processor` and
  `do_xcom_push` work as for `BashOperator`).
- **Exit codes:** exit code 99 skips the task (`skip_on_exit_code`), any other non-zero code fails
  it, including pixi's own failures, such as an environment that cannot be solved.
- **Dependencies:** tasks in the task's `depends-on` run first, in the same Airflow task and log.
- **Environment variables:** `env`, `append_env`, `env_vars`, `env_from_variables`,
  `env_from_connections`, `pixi_conn_id` and the cache directory Variables work as for the
  [Pixi Bash Operator](bash.md#environment-variables), with the same precedence.

## Task arguments

Pixi matches `task_args` against the task's definition, as described in
[pixi's task arguments](https://pixi.sh/latest/workspace/advanced_tasks/):

- A task that declares `args` gets them in the order they are declared, and uses them in its `cmd`
  as `{{ name }}`. Arguments with a `default` can be left out. More arguments than declared fail the
  run, unless the extra ones follow `--`: everything after `--` is passed to the command unchanged.
- A task without `args` gets the arguments appended to its command, `--` included, each as one
  argument.

!!! warning "Values of declared `args` are interpreted by pixi's task shell"

    Pixi puts the value of a declared argument into `cmd` as plain text, without quoting, and then runs
    `cmd` with its task shell. A value with a space becomes two words, and `;`, `|`, `&&` or `$` in a
    value are interpreted: with `cmd = "python report.py --day {{ day }}"`, the value
    `2026-01-01; rm -rf data` runs `rm -rf data`. Don't pass values that someone else controls, such as
    `dag_run.conf`, Params a user can set when triggering, or the output of a task that reads outside
    input, to a task that declares `args`. Validate them first, or use a task without `args`, or pass
    them after `--`, where they arrive unchanged. Quoting the placeholder in `cmd` (`'{{ day }}'`) keeps
    spaces together, but a value that contains `'` still breaks out of it.

```python
# python train.py --epochs 3
PixiProjectTaskOperator(task_id="train", pixi_project_path="/repo", task="train", task_args=["--epochs", "3"])

# python report.py --day 2026-01-01 --format html --verbose
PixiProjectTaskOperator(
    task_id="report",
    pixi_project_path="/repo",
    task="report",
    task_args=["{{ ds }}", "--", "--verbose"],
)
```

`task` and `task_args` are templated. `task_args` must be a list; it can also be the output of
another task that returns a list:

```python
@task
def arguments() -> list[str]:
    return ["--epochs", "3"]


PixiProjectTaskOperator(task_id="train", pixi_project_path="/repo", task="train", task_args=arguments())
```

A `task_args` item that ends with `.sh` or `.bash` is an argument, not a template file to load.

## Working directory

Pixi runs a task in the workspace root, the directory of the manifest, or in the task's own `cwd`
relative to it. The operator's `cwd` (default: the manifest's directory) is where pixi itself starts,
so it matters only for names that are not tasks.

## Names that are not tasks

As on the command line, `pixi run` runs a name that is not a task as a command of the environment, so
`task="pytest"` runs `pytest` from the environment when the manifest has no `pytest` task. A
misspelled task name therefore fails with a "command not found" error from the run rather than an
error that the task is missing.

## No TaskFlow decorator

There is no `@task.pixi_task`. A decorated function would only return the task's arguments; pass them
as `task_args` instead, from a template or from another task's output as shown above.

## Templated fields

`task`, `task_args`, `env`, `cwd`, `pixi_project_path`, `pixi_toml_path`, `environment`, `lock_mode`,
`pixi_cache_dir_variable`, `uv_cache_dir_variable` and `pip_cache_dir_variable`. `env_vars`,
`env_from_variables`, `env_from_connections` and `pixi_conn_id` are not templated.

## Reference

For the full list of parameters, see the
[`PixiProjectTaskOperator` API reference][airflow.providers.pixi.operators.project_task.PixiProjectTaskOperator].
