# apache-airflow-providers-pixi

## apache-airflow-providers-pixi package

Run Python callables inside [Pixi](https://pixi.sh)-managed environments, so a task can use
packages from conda-forge and PyPI that are not installed on the Airflow worker.

## Provider package

This package is for the `pixi` provider. All classes for this provider package are included in the
`airflow.providers.pixi` Python package:

| Class | Decorator | Description |
|---|---|---|
| [`PixiOperator`](operators/pixi.md) | [`@task.pixi`](decorators/pixi.md) | runs a Python callable in a Pixi environment on the worker |
| [`PixiBashOperator`](operators/bash.md) | `@task.pixi_bash` | runs a Bash command in a Pixi environment, like `BashOperator` |
| [`PixiProjectTaskOperator`](operators/project_task.md) | - | runs a task from the manifest's `[tasks]` with `pixi run <task>` |
| [`PixiKubernetesPodOperator`](operators/kubernetes.md) | `@task.pixi_kubernetes` | runs a Python callable in a Pixi environment in a Kubernetes pod |
| [`PixiDockerOperator`](operators/docker.md) | `@task.pixi_docker` | runs a Python callable in a Pixi environment in a Docker container |
| [`PixiExternalPythonOperator`](operators/external.md) | `@task.pixi_external` | runs a Python callable with the Python of an installed Pixi environment, without pixi, like `ExternalPythonOperator` |
| [`PixiBranchOperator`](operators/branch.md) | `@task.pixi_branch` | chooses the tasks to follow with a Python callable run in a Pixi environment, like `BranchPythonVirtualenvOperator` |
| [`PixiShortCircuitOperator`](operators/branch.md#short-circuit) | `@task.pixi_short_circuit` | skips the tasks downstream when a Python callable, run in a Pixi environment, returns a falsy value, like `ShortCircuitOperator` |
| [`PixiSensor`](sensors/pixi.md) | `@task.pixi_sensor` | waits for a Python callable, run in a Pixi environment, to return a truthy value |

The [`pixi` connection type](connections/pixi.md) holds credentials for private conda channels and
PyPI indexes, and [`PixiCallableError`][airflow.providers.pixi.exceptions.PixiCallableError] is the
error a task fails with when the function raises inside the environment.

Other providers can build their own operators and decorators on these; see
[Building on the Pixi Operator](extending.md).

## Installation

Install it on top of an existing Airflow installation:

```bash
pip install apache-airflow-providers-pixi
pip install "apache-airflow-providers-pixi[cncf.kubernetes]"
pip install "apache-airflow-providers-pixi[docker]"
```

The `cncf.kubernetes` extra adds the Kubernetes pod operator, and the `docker` extra the Docker
operator. For the minimum Airflow version
supported, see [Requirements](#requirements) below.

The workers also need [Pixi](https://pixi.sh/latest/installation/), installed for example in the
worker image. The provider never installs it; see [Pixi binary](operators/pixi.md#pixi-binary) and the
[deployment guide](deployment.md).

## Requirements

The minimum Apache Airflow version supported by this provider distribution is `3.1.2`. Airflow 3.0
is not supported. The provider floors are the versions pinned by Airflow 3.1.2's constraints file, so
the constraints file of any supported Airflow release can be used.

| PIP package | Version required |
|---|---|
| `apache-airflow` | `>=3.1.2` |
| `apache-airflow-providers-standard` | `>=1.9.1` |
| `packaging` | `>=22` |
| `tomlkit` | `>=0.12` |

With the `cncf.kubernetes` extra:

| PIP package | Version required |
|---|---|
| `apache-airflow-providers-cncf-kubernetes` | `>=10.9.0` |

With the `docker` extra:

| PIP package | Version required |
|---|---|
| `apache-airflow-providers-docker` | `>=4.4.4` |

On the workers:

| Tool | Version required |
|---|---|
| [`pixi`](https://pixi.sh/latest/installation/) | `>=0.81.0` |

Python 3.10, 3.11, 3.12, 3.13 and 3.14 are supported; Airflow 3.1 itself runs on Python 3.13 at
most, so Python 3.14 needs Airflow 3.2 or newer. CI runs the unit tests against the latest patch of
each supported Airflow minor with its constraints file, and against the lowest allowed versions; see
[Testing against other Airflow versions](installing-from-sources.md#testing-against-other-airflow-versions).
The Python inside the Pixi environment is independent of the worker's: it is whatever the manifest
asks for, from Python 3.10 on, the oldest Python Airflow 3.1 supports.

## Quick start

```python
from airflow.sdk import dag, task


@dag
def my_dag():
    @task
    def epochs() -> int:
        return 3

    @task.pixi(dependencies={"python": "3.12.*", "numpy": ">=2"})
    def train(epochs: int) -> float:
        import numpy as np  # imports go inside the function: it runs in the Pixi environment

        return float(np.random.rand(epochs).mean())

    train(epochs())


my_dag()
```

Continue with the [Pixi Operator](operators/pixi.md) guide for the ways to point a task at an
environment, and how arguments, return values, logs and timeouts work.
