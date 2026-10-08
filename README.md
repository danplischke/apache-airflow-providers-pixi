# apache-airflow-providers-pixi

Apache Airflow 3 provider to run Python callables inside [Pixi](https://pixi.sh)-managed
environments: `PixiOperator` and the `@task.pixi` TaskFlow decorator.

**Documentation:** https://danplischke.github.io/apache-airflow-providers-pixi/

## Installation

```bash
pip install apache-airflow-providers-pixi
pip install "apache-airflow-providers-pixi[cncf.kubernetes]"  # adds PixiKubernetesPodOperator
```

Requires `apache-airflow>=3.0`, and [Pixi](https://pixi.sh/latest/installation/) 0.81.0 or
newer installed on the workers, for example in the worker image. The provider never installs
pixi itself: a task fails if `pixi` is not on `PATH` (or at `pixi_binary`) or is older than
0.81.0.

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

### Bash, Kubernetes and sensors

```python
from airflow.sdk import task

from airflow.providers.pixi.operators.bash import PixiBashOperator

# `pixi run --manifest-path /repo bash -c '...'`: every part of the command runs in the environment
PixiBashOperator(
    task_id="train_cli",
    pixi_project_path="/repo",
    environment="{{ params.env }}",  # templated, so it can be picked per run
    bash_command="python train.py --epochs 3 | tee train.log",
)


@task.pixi_kubernetes(image="ghcr.io/prefix-dev/pixi:0.81.0", requirements=["pandas"], namespace="jobs")
def summarize(path: str) -> dict:
    import pandas as pd

    return pd.read_parquet(path).describe().to_dict()


@task.pixi_sensor(requirements=["s3fs"], poke_interval=60, mode="reschedule", env_cache_path="/var/cache/pixi-airflow")
def landed(path: str) -> bool:
    import s3fs

    return s3fs.S3FileSystem().exists(path)
```

## Operators and decorators

| | |
|---|---|
| `PixiOperator` / `@task.pixi` | runs a Python callable in a Pixi environment on the worker |
| `PixiBashOperator` / `@task.pixi_bash` | runs a Bash command with `pixi run ... bash -c`, like `BashOperator` |
| `PixiKubernetesPodOperator` / `@task.pixi_kubernetes` | runs a Python callable in a Pixi environment in a Kubernetes pod (`[cncf.kubernetes]` extra) |
| `PixiSensor` / `@task.pixi_sensor` | waits for a Python callable, run in a Pixi environment, to return a truthy value |

All of them choose the environment the same way (below). Templated fields: `op_args`,
`op_kwargs`, `pixi_project_path`, `pixi_toml_path`, `environment`, `requirements` and the cache
directory Variable names. `pixi_toml_path` must point at a `pixi.toml` or
`pyproject.toml`; pixi uses exactly that file.

### Inline manifest options

- **Workspace:** `channels` (default `["conda-forge"]`), `platforms` (default `linux-64`,
  `osx-64`, `osx-arm64`, `win-64`), `workspace_name`
- **Conda:** `dependencies` (dict, or list of MatchSpecs)
- **PyPI:** `pypi_dependencies`, `requirements` (pip requirement strings, as for
  `@task.virtualenv`), `pypi_options`. With PyPI packages but no `python` dependency, the
  environment gets the worker's Python version.
- **Multiple environments:** `environments` (dict), `feature` (dict of feature configs)

Same structure as the [Pixi manifest](https://pixi.sh/dev/reference/pixi_manifest/).

By default an inline environment is built in a temporary directory for each run and removed
afterwards (keep it for inspection with `cleanup_temp_manifest=False`). With
`env_cache_path`, it lives in `<env_cache_path>/pixi-<hash of the manifest>` and later runs
of the same manifest reuse it, like `venv_cache_path` of `PythonVirtualenvOperator`. Remove
old directories there yourself; concurrent runs of one manifest are safe.

### Environment variables

The run inherits the worker's environment; `env_vars` adds to or overrides it. It is not
templated, so secrets set there are never rendered into the UI.

### Timeouts and killing

There is no built-in time limit: set Airflow's `execution_timeout`. When it expires, or the
task is killed, the operator stops pixi and every process it started.

### Environment

If the manifest defines several environments (for example `default`, `test` and `cuda`
under `[environments]`), set `environment` to the one to use. Omit it for Pixi's default.

### Pixi binary

`pixi_binary` (default `pixi`) is a name on `PATH` or a path. Before the first run with a
binary, the operator checks `pixi --version` against the minimum, 0.81.0
(`airflow.providers.pixi.utils.pixi.MIN_PIXI_VERSION`).

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

## Building on this provider

Other providers can build their own operators and task decorators on `PixiOperator` and
`@task.pixi`, with the extension points of `@task.virtualenv` (`get_python_source()`,
`op_kwargs`, `env_vars`, `requirements`), so a mixin written for `@task.virtualenv` usually works
on `@task.pixi` unchanged. See
[Building on the Pixi Operator](https://danplischke.github.io/apache-airflow-providers-pixi/extending/).

## Tests

```bash
uv sync --group dev
pytest tests/unit                                  # a fake pixi runs the command with this Python
PIXI_INTEGRATION_TEST=1 pytest tests/integration   # real pixi on PATH, needs conda-forge access

export AIRFLOW_HOME=/tmp/airflow-e2e AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/tests/system/pixi
airflow db migrate
PIXI_E2E_TEST=1 pytest tests/system/pixi           # dag.test(): real task runner and pixi
```

### Local Airflow

`just standalone` starts Airflow (`airflow standalone`, SQLite) at http://localhost:8080 without a
login, with the DAGs in [dev/dags](dev/dags): every operator and decorator against the sample project
in [dev/project](dev/project), and one for a local Kubernetes cluster. It keeps its state in
`.airflow/`; `just airflow-reset` deletes it, `just airflow <command>` runs the Airflow CLI against it
(for example `just airflow dags test pixi_showcase`), and `AIRFLOW_PORT` changes the port. Pixi has to
be installed (`brew install pixi`).

### Recipes

With [just](https://just.systems), `just dev` sets everything up and `just` lists the recipes:
`just test`, `just test-integration` and `just test-system` run the tiers above, `just check`
runs what CI runs, and `just bump <version>` / `just release <version>` cut a release.

## License

Apache-2.0
