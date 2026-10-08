# `apache-airflow-providers-pixi`

## apache-airflow-providers-pixi package

[Pixi](https://pixi.sh)

Run Python callables inside [Pixi](https://pixi.sh)-managed environments, so a task can use
packages from conda-forge and PyPI that are not installed on the Airflow worker.

## Provider package

This package is for the `pixi` provider. All classes for this provider package are included in the
`airflow.providers.pixi` Python package:

| Class | Description |
|---|---|
| [`PixiOperator`](operators/pixi.md) | runs a Python callable in a Pixi environment |
| [`@task.pixi`](decorators/pixi.md) | TaskFlow variant of `PixiOperator` |

Other providers can build their own operators and decorators on these; see
[Building on the Pixi Operator](extending.md).

## Installation

You can install this package on top of an existing Airflow installation via
`pip install apache-airflow-providers-pixi`. For the minimum Airflow version supported, see
[Requirements](#requirements) below.

The workers also need the [Pixi CLI](https://pixi.sh/latest/installation/), for example
`brew install pixi`. If it is not on `PATH`, the operator installs it with the official install
script on first use; see [Auto-install Pixi](operators/pixi.md#auto-install-pixi).

## Requirements

The minimum Apache Airflow version supported by this provider distribution is `3.0`.

| PIP package | Version required |
|---|---|
| `apache-airflow` | `>=3.0` |

Python 3.10, 3.11, 3.12, 3.13 and 3.14 are supported. The Python inside the Pixi environment is
independent of the worker's: it is whatever the manifest asks for.

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
