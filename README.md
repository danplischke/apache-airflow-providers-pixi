# pixi-airflow

Apache Airflow 3 provider to run Python callables inside [Pixi](https://pixi.sh)-managed
environments: `PixiOperator` and the `@task.pixi` TaskFlow decorator.

## Installation

```bash
pip install pixi-airflow
```

Requires `apache-airflow>=3.0` and the [Pixi CLI](https://pixi.sh) on the workers (for
example `brew install pixi`). If Pixi is not on `PATH`, the operator installs it with the
official install script unless you pass `auto_install_pixi=False`.

## Usage

```python
from airflow.sdk import DAG

from airflow.providers.pixi import PixiOperator

with DAG("my_pipeline") as dag:
    # existing Pixi project: a directory with pixi.toml or pyproject.toml
    train = PixiOperator(
        task_id="train",
        pixi_project_path="/path/to/pixi/project",
        python_callable="mymodule:train",
        op_kwargs={"epochs": 3},
        environment="cuda",  # optional: one of the manifest's environments
    )

    # explicit manifest file
    evaluate = PixiOperator(
        task_id="evaluate",
        pixi_toml_path="/repo/pixi.toml",
        environment="test",
        python_callable="mymodule:evaluate",
    )

    # inline manifest, same options as pixi.toml; env_cache_path keeps the environment for later runs
    report = PixiOperator(
        task_id="report",
        dependencies={"python": ">=3.10", "numpy": "*"},
        pypi_dependencies={"pandas": ">=2.0"},
        channels=["conda-forge"],
        platforms=["linux-64", "osx-arm64"],
        env_cache_path="/var/cache/pixi-airflow",
        python_callable="mymodule:report",
        op_args=[evaluate.output],  # upstream XCom, resolved at run time
    )

    train >> evaluate
```

Specify the manifest in exactly one way: `pixi_project_path` (directory), `pixi_toml_path`
(file), or inline (`dependencies` / `pypi_dependencies`).

`@task.pixi` is registered on Airflow's `task` object once the provider is installed. It
accepts the same arguments as `PixiOperator`:

```python
from airflow.sdk import dag, task


@dag
def my_dag():
    @task
    def epochs() -> int:
        return 3

    @task.pixi(pixi_project_path="/path/to/pixi/project", environment="cuda")
    def train(epochs: int) -> float:
        import torch  # imports go inside the function: it runs in the Pixi environment

        return torch.rand(epochs).mean().item()

    train(epochs())
```

How it works:

- At run time the operator calls `pixi run --manifest-path <manifest> python -c <runner>`,
  with the manifest's directory as working directory.
- A function is shipped as source, as `@task.virtualenv` does, so it can be defined in the
  DAG file. It must be self-contained: imports inside it, and no variables from enclosing
  functions (pass them as arguments). A `"module.path:name"` string is imported inside the
  environment instead, for code that lives in the project or is installed there.
- `op_args` / `op_kwargs` are templated, so upstream XComs and Jinja work. They and the
  return value cross into and out of the environment as JSON, or with `serializer="pickle"`
  for values JSON cannot hold (their types must be importable on both sides). A value that
  does not fit the serializer fails the task instead of being converted.
- Everything the run prints, pixi's messages included, is streamed to the task log.
- The return value is the task's XCom.

## Operators and decorators

| | |
|---|---|
| `PixiOperator(task_id, python_callable, op_args=None, op_kwargs=None, ...)` | runs the callable in a Pixi environment |
| `@task.pixi(...)` | TaskFlow variant of `PixiOperator` |

Templated fields: `op_args`, `op_kwargs`, `pixi_project_path`, `pixi_toml_path` and the
cache directory Variable names. `pixi_toml_path` must point at a `pixi.toml` or
`pyproject.toml`; pixi uses exactly that file.

### Inline manifest options

- **Workspace:** `channels` (default `["conda-forge"]`), `platforms` (default `linux-64`,
  `osx-64`, `osx-arm64`, `win-64`), `name`
- **Conda:** `dependencies` (dict, or list of MatchSpecs)
- **PyPI:** `pypi_dependencies`, `pypi_options`
- **Multiple environments:** `environments` (dict), `feature` (dict of feature configs)

Same structure as the [Pixi manifest](https://pixi.sh/dev/reference/pixi_manifest/).

By default an inline environment is built in a temporary directory for each run and removed
afterwards (keep it for inspection with `cleanup_temp_manifest=False`). With
`env_cache_path`, it lives in `<env_cache_path>/pixi-<hash of the manifest>` and later runs
of the same manifest reuse it, like `venv_cache_path` of `PythonVirtualenvOperator`. Remove
old directories there yourself; concurrent runs of one manifest are safe.

### Timeouts and killing

There is no built-in time limit: set Airflow's `execution_timeout`. When it expires, or the
task is killed, the operator stops pixi and every process it started.

### Environment

If the manifest defines several environments (for example `default`, `test` and `cuda`
under `[environments]`), set `environment` to the one to use. Omit it for Pixi's default.

### Auto-install Pixi

If the Pixi binary (`pixi_binary`, default `pixi`) is not on `PATH`, the operator runs the
[official install script](https://pixi.sh) (Unix: `curl | sh`, Windows: PowerShell
`irm | iex`) and uses the installed binary (`~/.pixi/bin` on Unix,
`%LOCALAPPDATA%\pixi\bin` on Windows). Pass `auto_install_pixi=False` to fail instead.

### Cache directories (Airflow Variables)

Point Pixi, uv and pip at shared cache directories through Airflow Variables. Set the
Variable names on the operator (or via `default_args`), then create the Variables in the
Airflow UI or with `airflow variables set <name> <path>`:

| Operator parameter | Env var set in the subprocess | Typical Variable name |
|---|---|---|
| `pixi_cache_dir_variable` | `PIXI_CACHE_DIR` | `pixi_cache_dir` |
| `uv_cache_dir_variable` | `UV_CACHE_DIR` | `uv_cache_dir` |
| `pip_cache_dir_variable` | `PIP_CACHE_DIR` | `pip_cache_dir` |

The Variables are read at task run time; a missing Variable leaves the env var unset, so
the tool's default applies.

## Tests

```bash
uv sync --group dev
pytest tests/unit                                  # a fake pixi runs the command with this Python
PIXI_INTEGRATION_TEST=1 pytest tests/integration   # real pixi on PATH, needs conda-forge access

export AIRFLOW_HOME=/tmp/airflow-e2e AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/tests/system/pixi
airflow db migrate
PIXI_E2E_TEST=1 pytest tests/system                # dag.test(): real task runner and pixi
```

## License

Apache-2.0
