# Pixi Kubernetes Pod Operator

Use the [`PixiKubernetesPodOperator`][airflow.providers.pixi.operators.kubernetes.PixiKubernetesPodOperator]
to run a Python callable inside a [Pixi](https://pixi.sh) environment in a Kubernetes pod. It combines
the [Pixi Operator](pixi.md) with Airflow's `KubernetesPodOperator`, and accepts the arguments of
both, except `cmds` and `arguments`, which run the callable.

It needs the `cncf.kubernetes` extra:

```bash
pip install "apache-airflow-providers-pixi[cncf.kubernetes]"
```

## Using the operator

```python
from airflow.providers.pixi.operators.kubernetes import PixiKubernetesPodOperator


def summarize(path: str) -> dict:
    import pandas as pd

    return pd.read_parquet(path).describe().to_dict()


PixiKubernetesPodOperator(
    task_id="summarize",
    namespace="jobs",
    requirements=["pandas", "pyarrow"],
    python_callable=summarize,
    op_args=["/data/input.parquet"],
)
```

The callable works as for the [Pixi Operator](pixi.md#using-the-operator): a self-contained function
shipped as source, or a `"module.path:callable_name"` string imported in the pod. Arguments and the
return value travel as JSON, or with `serializer="pickle"`. The return value is the task's XCom;
unlike `KubernetesPodOperator`, `do_xcom_push` defaults to `True`.

## The image

The default image is `ghcr.io/prefix-dev/pixi:0.81.0`, the official pixi image in the minimum
supported version. Your own image needs:

- pixi 0.81.0 or newer, as `pixi` on `PATH` or at `pixi_binary`. The pod checks the version before
  running and fails if it is older.
- `sh`, GNU `sort` and `base64`, as in Debian and Ubuntu based images.
- for `pixi_project_path` or `pixi_toml_path`, the project, at that path in the image.

Paths (`pixi_project_path`, `pixi_toml_path`, `env_cache_path`) are paths in the pod.

## Inline environments

An inline manifest (`dependencies`, `pypi_dependencies`, `requirements`) is written in the pod and
solved there on each run. To reuse it, mount a volume and point `env_cache_path` at it:

```python
from kubernetes.client import models as k8s

PixiKubernetesPodOperator(
    task_id="summarize",
    requirements=["pandas"],
    env_cache_path="/cache/pixi",
    volumes=[k8s.V1Volume(name="cache", persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="pixi-cache"))],
    volume_mounts=[k8s.V1VolumeMount(name="cache", mount_path="/cache")],
    python_callable=summarize,
)
```

## How it works

The function's source, its arguments and an inline manifest reach the pod in environment
variables. The pod runs a short shell script that checks the pixi version, writes them to files, and
runs `pixi run --manifest-path <manifest> python -c <runner>`. The return value is written to
`/airflow/xcom/return.json`, where `KubernetesPodOperator`'s XCom sidecar picks it up. The pod's log is
streamed to the task log.

## `@task.pixi_kubernetes`

```python
from airflow.sdk import task


@task.pixi_kubernetes(requirements=["pandas"], namespace="jobs")
def summarize(path: str) -> dict:
    import pandas as pd

    return pd.read_parquet(path).describe().to_dict()
```

It accepts every `PixiKubernetesPodOperator` argument. As for `@task.pixi`, the function must be
self-contained; see [Writing the function](../decorators/pixi.md#writing-the-function).
