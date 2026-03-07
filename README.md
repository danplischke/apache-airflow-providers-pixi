# apache-airflow-providers-pixi

Apache Airflow 3 provider to run Python callables inside [Pixi](https://github.com/prefix-dev/pixi)-managed environments. Provides **PixiOperator** and the **@task.pixi** TaskFlow decorator.

## Requirements

- **Apache Airflow 3.x**
- **Pixi CLI** on `PATH` where workers run (install from [pixi.sh](https://pixi.sh) or `brew install pixi`). If Pixi is not found, the operator can auto-install it via the official install script (`auto_install_pixi=True`, default).

## Installation

```bash
pip install apache-airflow-providers-pixi
# or from source
pip install -e /path/to/pixi_operator
```

## Usage

### Operator: PixiOperator

Run a Python callable (by module path or reference) in a Pixi environment. You must specify the manifest in exactly one of these ways:

1. **Project path:** directory containing `pixi.toml` or `pyproject.toml`
2. **Toml file path:** path to a `pixi.toml` or `pyproject.toml` file
3. **Inline:** pass dependencies and options in the same format as [pixi.toml](https://pixi.sh/dev/reference/pixi_manifest/) (operator writes a temporary manifest)

```python
from airflow_providers_pixi.operators.pixi import PixiOperator

# Existing Pixi project
PixiOperator(
    task_id="run_in_pixi",
    pixi_project_path="/path/to/pixi/project",
    python_callable="mymodule:my_func",
    op_kwargs={"key": "value"},
    environment="cuda",  # optional: select Pixi environment when manifest has multiple
)

# Explicit toml path
PixiOperator(
    task_id="run_in_pixi",
    pixi_toml_path="/repo/pixi.toml",
    environment="test",
    python_callable="mymodule:my_func",
)

# Inline dependencies (conda + PyPI, same options as pixi.toml)
PixiOperator(
    task_id="inline_env",
    dependencies={"python": ">=3.10", "numpy": "*"},
    pypi_dependencies={"pandas": ">=2.0"},
    channels=["conda-forge"],
    platforms=["linux-64", "osx-64"],
    environment="default",
    python_callable="mymodule:other_func",
)
```

### Decorator: @task.pixi

After installing the provider, `@task.pixi` is registered on Airflow’s `task` object. Use it from `airflow.decorators`:

```python
from airflow.decorators import dag, task

@dag(...)
def my_dag():
    @task.pixi(pixi_project_path="/path/to/pixi/project", environment="cuda")
    def run_in_pixi(x: int) -> int:
        return x + 1

    run_in_pixi(2)
```

Parameters (e.g. `pixi_project_path`, `pixi_toml_path`, `dependencies`, `pypi_dependencies`, `channels`, `platforms`, `environment`, `environments`, `feature`, etc.) are the same as for **PixiOperator**.

## Manifest options (inline)

When using inline manifest (no path), you can pass:

- **Workspace:** `channels`, `platforms`, `name` (optional)
- **Conda:** `dependencies` (dict or list of MatchSpecs)
- **PyPI:** `pypi_dependencies`, optional `pypi_options`
- **Multiple environments:** `environments` (dict), optional `feature` (dict of feature configs)

Same structure as [Pixi manifest](https://pixi.sh/dev/reference/pixi_manifest/).

## Environment parameter

If your `pixi.toml` defines multiple environments (e.g. `[environments]` with `default`, `test`, `cuda`), set **environment** to the name to use. Omit for Pixi’s default.

## Callable and arguments

- **python_callable:** Either a `"module.path:callable_name"` string or a callable (resolved to module:name at init). The callable must be **importable** in the Pixi environment (its code and dependencies live in or are installed in that environment).
- **op_args** / **op_kwargs:** Passed to the callable; must be **JSON-serializable** (they are written to a temp file and read inside the Pixi process).

Return value is pushed to XCom and available to downstream tasks.

## Auto-install Pixi

By default, if the Pixi binary is not on `PATH`, the operator runs the [official install script](https://pixi.sh) (Unix: `curl | sh`, Windows: PowerShell `irm | iex`) and uses the installed binary (`~/.pixi/bin` on Unix, `%LOCALAPPDATA%\\pixi\\bin` on Windows). Set `auto_install_pixi=False` to disable and fail instead when Pixi is missing.

## Cache directories (Airflow Variables)

You can point Pixi, uv, and pip at custom cache directories using Airflow Variables. Set the Variable names on the operator (or via default_args), then create the Variables in the Airflow UI or via `airflow variables set <name> <path>`.

| Operator parameter              | Env var set in subprocess | Typical Variable name   |
|--------------------------------|---------------------------|-------------------------|
| `pixi_cache_dir_variable`      | `PIXI_CACHE_DIR`         | e.g. `pixi_cache_dir`   |
| `uv_cache_dir_variable`        | `UV_CACHE_DIR`           | e.g. `uv_cache_dir`     |
| `pip_cache_dir_variable`       | `PIP_CACHE_DIR`          | e.g. `pip_cache_dir`    |

Example: set Airflow Variable `pixi_cache_dir` = `/shared/cache/pixi`, then use `PixiOperator(..., pixi_cache_dir_variable="pixi_cache_dir")`. The value is read at task runtime; if the Variable is missing, the env var is not set (tool defaults apply).

## Development

Install dev deps: `pip install -e ".[dev]"`. Run checks:

```bash
ruff check airflow_providers_pixi tests
ruff format airflow_providers_pixi tests
pytest tests/
pyright airflow_providers_pixi tests
mypy airflow_providers_pixi
```

## License

Apache-2.0
