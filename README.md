# apache-airflow-providers-pixi

An Apache Airflow 3 provider that runs tasks inside [Pixi](https://pixi.sh)-managed environments,
on the worker, in a Kubernetes pod or in a Docker container, so a task can use conda-forge and PyPI packages that are not
installed on the worker.

Documentation: https://danplischke.github.io/apache-airflow-providers-pixi/
(for LLMs and coding agents: [llms.txt](https://danplischke.github.io/apache-airflow-providers-pixi/llms.txt))

## Installation

```bash
pip install apache-airflow-providers-pixi
pip install "apache-airflow-providers-pixi[cncf.kubernetes]"  # adds PixiKubernetesPodOperator
pip install "apache-airflow-providers-pixi[docker]"  # adds PixiDockerOperator
```

Requires Apache Airflow 3.1.2 or newer and Python 3.10 to 3.14 (3.14 needs Airflow 3.2). The
workers need [pixi](https://pixi.sh/latest/installation/) 0.81.0 or newer on `PATH`; the provider
never installs it. See [Requirements](https://danplischke.github.io/apache-airflow-providers-pixi/#requirements)
and the [deployment guide](https://danplischke.github.io/apache-airflow-providers-pixi/deployment/).

## Quick start

```python
from airflow.sdk import dag, task


@dag
def my_dag():
    @task.pixi(dependencies={"python": "3.12.*"}, pypi_dependencies=["pandas>=2"])
    def summarize(n: int) -> float:
        import pandas as pd  # imports go inside the function: it runs in the Pixi environment

        return float(pd.Series(range(n)).mean())

    summarize(3)


my_dag()
```

Pass `pixi_project_path` or `pixi_toml_path` instead to use an existing Pixi project. The
[Pixi Operator](https://danplischke.github.io/apache-airflow-providers-pixi/operators/pixi/) guide
covers the ways to choose an environment, arguments and return values, the Airflow context,
environment variables, errors, timeouts and caches.

## Operators, decorators and sensor

| Class / decorator | What it does |
|---|---|
| [`PixiOperator`](https://danplischke.github.io/apache-airflow-providers-pixi/operators/pixi/) / [`@task.pixi`](https://danplischke.github.io/apache-airflow-providers-pixi/decorators/pixi/) | runs a Python callable in a Pixi environment on the worker |
| [`PixiBashOperator`](https://danplischke.github.io/apache-airflow-providers-pixi/operators/bash/) / `@task.pixi_bash` | runs a Bash command in a Pixi environment |
| [`PixiProjectTaskOperator`](https://danplischke.github.io/apache-airflow-providers-pixi/operators/project_task/) | runs a task from the manifest's `[tasks]` |
| [`PixiKubernetesPodOperator`](https://danplischke.github.io/apache-airflow-providers-pixi/operators/kubernetes/) / `@task.pixi_kubernetes` | runs a Python callable in a Pixi environment in a Kubernetes pod |
| [`PixiDockerOperator`](https://danplischke.github.io/apache-airflow-providers-pixi/operators/docker/) / `@task.pixi_docker` | runs a Python callable in a Pixi environment in a Docker container |
| [`PixiExternalPythonOperator`](https://danplischke.github.io/apache-airflow-providers-pixi/operators/external/) / `@task.pixi_external` | runs a Python callable with the Python of an installed Pixi environment, without pixi |
| [`PixiBranchOperator`](https://danplischke.github.io/apache-airflow-providers-pixi/operators/branch/) / `@task.pixi_branch` | chooses the downstream tasks to follow |
| [`PixiShortCircuitOperator`](https://danplischke.github.io/apache-airflow-providers-pixi/operators/branch/#short-circuit) / `@task.pixi_short_circuit` | skips downstream tasks when the callable returns a falsy value |
| [`PixiSensor`](https://danplischke.github.io/apache-airflow-providers-pixi/sensors/pixi/) / `@task.pixi_sensor` | waits until the callable returns a truthy value |

Credentials for private conda channels and PyPI indexes go in a
[`pixi` connection](https://danplischke.github.io/apache-airflow-providers-pixi/connections/pixi/).
Other providers can build on these operators; see
[Building on the Pixi Operator](https://danplischke.github.io/apache-airflow-providers-pixi/extending/).

## Development

With [uv](https://docs.astral.sh/uv/), [just](https://just.systems) and pixi installed:

- `just dev` installs the package with the dev and docs groups and the pre-commit hooks.
- `just test` runs the unit tests, with a fake pixi.
- `just check` runs what CI runs: lint, unit tests, docs build, package build and smoke test.
- `just standalone` starts a local Airflow at http://localhost:8080 with the DAGs in `dev/dags`.

`just` lists the other recipes. Integration, system and compatibility tests are described in
[System Tests](https://danplischke.github.io/apache-airflow-providers-pixi/system-tests/) and
[Installing from sources](https://danplischke.github.io/apache-airflow-providers-pixi/installing-from-sources/).

## License

MIT
