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

from pixi_airflow import PixiOperator

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

    # inline manifest, same options as pixi.toml; written to a temporary directory
    report = PixiOperator(
        task_id="report",
        dependencies={"python": ">=3.10", "numpy": "*"},
        pypi_dependencies={"pandas": ">=2.0"},
        channels=["conda-forge"],
        platforms=["linux-64", "osx-arm64"],
        python_callable="mymodule:report",
    )

    train >> evaluate >> report
```

Specify the manifest in exactly one way: `pixi_project_path` (directory), `pixi_toml_path`
(file), or inline (`dependencies` / `pypi_dependencies`).

`@task.pixi` is registered on Airflow's `task` object once the provider is installed. It
accepts the same arguments as `PixiOperator`:

```python
from airflow.sdk import dag, task

from mypkg.jobs import train  # importable on the worker and inside the Pixi environment


@dag
def my_dag():
    task.pixi(pixi_project_path="/path/to/pixi/project", environment="cuda")(train)(epochs=3)
```

How it works:

- `python_callable` is resolved to `"module.path:name"` when the task is created. At run
  time the operator calls `pixi run --manifest-path <project> python -c <runner>` with the
  project directory as working directory, so modules in that directory are importable.
- `op_args` / `op_kwargs` are written to a temporary JSON file that the runner reads, so
  they must be JSON-serializable; anything else arrives as its `str()`.
- The runner prints the return value as JSON on the last line of stdout; the operator
  parses it and pushes it to XCom.

## Operators and decorators

| | |
|---|---|
| `PixiOperator(task_id, python_callable, op_args=None, op_kwargs=None, ...)` | runs the callable in a Pixi environment |
| `@task.pixi(...)` | TaskFlow variant of `PixiOperator` |

`python_callable` is a `"module.path:callable_name"` string or a callable, and must be
importable inside the Pixi environment: its code lives in the project directory or is
installed in the environment. For `@task.pixi` this means a function defined in the DAG
file does not work, because Airflow imports DAG files under generated module names that
do not exist inside the environment.

### Inline manifest options

- **Workspace:** `channels` (default `["conda-forge"]`), `platforms` (default `linux-64`,
  `osx-64`, `osx-arm64`, `win-64`), `name`
- **Conda:** `dependencies` (dict, or list of MatchSpecs)
- **PyPI:** `pypi_dependencies`, `pypi_options`
- **Multiple environments:** `environments` (dict), `feature` (dict of feature configs)

Same structure as the [Pixi manifest](https://pixi.sh/dev/reference/pixi_manifest/). The
temporary directory is removed after the run unless `cleanup_temp_manifest=False`.

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
pytest tests/unit                                  # pixi mocked
PIXI_INTEGRATION_TEST=1 pytest tests/integration   # real pixi on PATH, needs conda-forge access
```

## License

Apache-2.0
