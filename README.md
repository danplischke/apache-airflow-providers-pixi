# apache-airflow-providers-pixi

Apache Airflow 3 provider to run Python callables, Bash commands, manifest tasks, branch choices and
sensor checks inside [Pixi](https://pixi.sh)-managed environments, on the worker or in a Kubernetes pod:
`PixiOperator`, `PixiBashOperator`, `PixiTaskOperator`, `PixiKubernetesPodOperator`,
`PixiBranchOperator`, `PixiShortCircuitOperator` and `PixiSensor`, and a `pixi` connection type for
private channels and indexes.

**Documentation:** https://danplischke.github.io/apache-airflow-providers-pixi/
(for LLMs and coding agents: [llms.txt](https://danplischke.github.io/apache-airflow-providers-pixi/llms.txt),
[llms-full.txt](https://danplischke.github.io/apache-airflow-providers-pixi/llms-full.txt))

## Installation

```bash
pip install apache-airflow-providers-pixi
pip install "apache-airflow-providers-pixi[cncf.kubernetes]"  # adds PixiKubernetesPodOperator
```

Requires Apache Airflow 3.1.2 or newer (`apache-airflow>=3.1.2`; Airflow 3.0 is not supported),
and [Pixi](https://pixi.sh/latest/installation/) 0.81.0 or newer installed on the workers, for
example in the worker image (see the
[deployment guide](https://danplischke.github.io/apache-airflow-providers-pixi/deployment/)). The
provider never installs pixi itself: a task fails if `pixi` is not on `PATH` (or at `pixi_binary`)
or is older than 0.81.0. Airflow's constraints files can be used for every supported Airflow release.

## Usage

```python
from airflow.sdk import DAG

from airflow.providers.pixi import PixiOperator

with DAG("my_pipeline") as dag:
    # existing Pixi project: a directory with pixi.toml or pyproject.toml; a relative path is
    # relative to the DAG file
    train = PixiOperator(
        task_id="train",
        pixi_project_path="/path/to/pixi/project",
        python_callable="mymodule:train",
        op_kwargs={"epochs": 3},
        environment="cuda",  # optional: one of the manifest's environments
        lock_mode="locked",  # use pixi.lock as it is; fail if it is out of date
    )

    # explicit manifest file
    evaluate = PixiOperator(
        task_id="evaluate",
        pixi_toml_path="/repo/pixi.toml",
        environment="test",
        python_callable="mymodule:evaluate",
    )

    # inline manifest: a list of packages; env_cache_path keeps the environment for later runs
    report = PixiOperator(
        task_id="report",
        dependencies={"python": ">=3.10", "numpy": "*"},
        pypi_dependencies={"pandas": ">=2.0"},
        channels=["conda-forge"],
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
    def train(epochs: int, ds=None) -> float:  # ds is filled from the Airflow context
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
- Parameters named after a context key (`ds`, `run_id`, `params`, `logical_date`, ...) are
  filled from the Airflow context, as with `@task.virtualenv`; dates arrive as ISO strings.
- Everything the run prints, pixi's messages included, is streamed to the task log. An
  exception in the function fails the task with `PixiCallableError`, which names the function,
  the exception and its message; `skip_on_exit_code` skips the task instead.
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


@task.pixi_kubernetes(image="ghcr.io/prefix-dev/pixi:0.81.0", pypi_dependencies=["pandas", "pyarrow"], namespace="jobs")
def summarize(path: str) -> dict:
    import pandas as pd

    return pd.read_parquet(path).describe().to_dict()


@task.pixi_sensor(pypi_dependencies=["s3fs"], poke_interval=60, mode="reschedule", env_cache_path="/var/cache/pixi-airflow")
def landed(path: str) -> bool:
    import s3fs

    return s3fs.S3FileSystem().exists(path)
```

## Operators and decorators

| | |
|---|---|
| `PixiOperator` / `@task.pixi` | runs a Python callable in a Pixi environment on the worker |
| `PixiBashOperator` / `@task.pixi_bash` | runs a Bash command with `pixi run ... bash -c`, like `BashOperator` |
| `PixiTaskOperator` | runs a task from the manifest's `[tasks]` with `pixi run <task> [args]` |
| `PixiKubernetesPodOperator` / `@task.pixi_kubernetes` | runs a Python callable in a Pixi environment in a Kubernetes pod (`[cncf.kubernetes]` extra) |
| `PixiBranchOperator` / `@task.pixi_branch` | chooses the tasks to follow with a Python callable run in a Pixi environment, like `BranchPythonVirtualenvOperator` |
| `PixiShortCircuitOperator` / `@task.pixi_short_circuit` | skips the tasks downstream when a Python callable, run in a Pixi environment, returns a falsy value, like `ShortCircuitOperator` |
| `PixiSensor` / `@task.pixi_sensor` | waits for a Python callable, run in a Pixi environment, to return a truthy value |

All of them choose the environment the same way (below). Templated fields: `op_args`,
`op_kwargs`, `pixi_project_path`, `pixi_toml_path`, `environment`, `lock_mode`, `pypi_dependencies` and
the cache directory Variable names. `pixi_toml_path` must point at a `pixi.toml` or
`pyproject.toml`; pixi uses exactly that file.

### Inline manifest options

- **Conda:** `dependencies` (dict, or list of MatchSpecs)
- **PyPI:** `pypi_dependencies`, a dict as under `[pypi-dependencies]`, or pip requirement strings as
  for `@task.virtualenv` (a list, or one string such as a rendered requirements file). With PyPI
  packages but no `python` dependency, the environment gets the worker's Python version.
- **Workspace:** `channels` (default `["conda-forge"]`), `platforms` (default: the platform of the
  machine running pixi; `linux-64` and `linux-aarch64` in a pod)

Anything beyond a list of packages, such as PyPI indexes (`[pypi-options]`), features or several
environments, goes in a [`pixi.toml`](https://pixi.sh/latest/reference/pixi_manifest/) passed as
`pixi_project_path` or `pixi_toml_path`.

By default an inline environment is built in a temporary directory for each run and removed
afterwards (keep it for inspection with `cleanup_temp_manifest=False`). With
`env_cache_path`, it lives in `<env_cache_path>/pixi-<hash of the manifest>` and later runs
of the same manifest reuse it, like `venv_cache_path` of `PythonVirtualenvOperator`. Remove
old directories there yourself; concurrent runs of one manifest are safe.

### Environment variables and credentials

The run inherits the worker's environment; `env_vars` adds to or overrides it. It is not
templated, so secrets set there are never rendered into the UI. `env_from_variables` and
`env_from_connections` set variables from Airflow Variables and Connections when the task runs,
for example `{"DB_PASSWORD": "warehouse.password"}`, and mask secret values in the logs.

For private conda channels and PyPI indexes, create a connection of type `pixi` and pass its id
as `pixi_conn_id`; see the
[Pixi Connection](https://danplischke.github.io/apache-airflow-providers-pixi/connections/pixi/) page.

### Timeouts and killing

There is no built-in time limit: set Airflow's `execution_timeout`. When it expires, or the
task is killed, the operator stops pixi and every process it started.

### Environment

If a project or manifest file defines several environments (for example `default`, `test` and
`cuda` under `[environments]`), set `environment` to the one to use. Omit it for Pixi's default.
An inline manifest has only the default environment and rejects `environment`.

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
`op_kwargs`, `env_vars`), plus `add_pypi_dependencies()` in place of appending to `requirements`, so a
mixin written for `@task.virtualenv` ports to `@task.pixi` with that one change. See
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
login, with the DAGs in [dev/dags](dev/dags): every operator, sensor and decorator that runs on the worker,
against the sample project in [dev/project](dev/project), and `@task.pixi_kubernetes` for a local
Kubernetes cluster. It keeps its state in
`.airflow/`; `just airflow-reset` deletes it, `just airflow <command>` runs the Airflow CLI against it
(for example `just airflow dags test pixi_showcase`), and `AIRFLOW_PORT` changes the port. Pixi has to
be installed (`brew install pixi`).

### Recipes

With [just](https://just.systems), `just dev` sets everything up and `just` lists the recipes:
`just test`, `just test-integration` and `just test-system` run the tiers above, `just test-k8s`
runs the pod tests in the current kubectl context (for example a kind cluster), `just test-compat`
and `just test-lowest` run the tests against each supported Airflow release with its
constraints file and against the lowest allowed versions, `just check` runs what CI runs, and
`just bump <version>` / `just release <version>` cut a release.

## License

MIT
