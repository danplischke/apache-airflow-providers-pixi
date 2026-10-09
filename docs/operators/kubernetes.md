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
    pypi_dependencies=["pandas", "pyarrow"],
    python_callable=summarize,
    op_args=["/data/input.parquet"],
)
```

The callable works as for the [Pixi Operator](pixi.md#using-the-operator): a self-contained function
shipped as source, or a `"module.path:callable_name"` string imported in the pod. Arguments and the
return value travel as JSON, or with `serializer="pickle"`. The return value is the task's XCom;
unlike `KubernetesPodOperator`, `do_xcom_push` defaults to `True`. Parameters named after context
keys, such as `ds`, `params` or `run_id`, get the task's values, as described in
[Airflow context](pixi.md#airflow-context).

`deferrable=True` works as for `KubernetesPodOperator`: the worker slot is freed while the pod runs,
and the return value, pickled or not, still becomes the task's XCom when the task resumes.

`env_vars` is `KubernetesPodOperator`'s: a dict of names and values, or a list of `k8s.V1EnvVar`. The
operator adds the variables that carry the callable to them. Anything else fails the task with a
`TypeError` before the pod is created. That includes the string a templated `env_vars` renders to
when the DAG doesn't set `render_template_as_native_obj=True`.

## The image

The default image is `ghcr.io/prefix-dev/pixi:0.81.0`, the official pixi image in the minimum
supported version. Your own image needs:

- pixi 0.81.0 or newer, as `pixi` on `PATH` or at `pixi_binary`. The pod checks the version before
  running and fails if it is older.
- `sh`, GNU `sort` and `base64`, as in Debian and Ubuntu based images.
- for `pixi_project_path` or `pixi_toml_path`, the project, at that path in the image.

Paths (`pixi_project_path`, `pixi_toml_path`, `env_cache_path`) are paths in the pod. A relative
path is relative to the image's working directory (its `WORKDIR`), not to the DAG file as on a
worker.

For a project in the image, `lock_mode="locked"` or `"frozen"` makes the pod install exactly what the
image's `pixi.lock` pins, instead of solving again when the lock file is out of date. See
[The lock file](pixi.md#the-lock-file). Installing the environment when the image is built saves the
install on every run. Build on the official image so pixi is already in it:

```dockerfile
FROM ghcr.io/prefix-dev/pixi:0.81.0

WORKDIR /app
COPY pixi.toml pixi.lock ./
RUN pixi install --locked
COPY . .
```

```python
PixiKubernetesPodOperator(
    task_id="train",
    image="registry.example.com/pipelines/train:1.4",
    pixi_project_path="/app",
    lock_mode="locked",
    python_callable="jobs:train",
)
```

Copying `pixi.toml` and `pixi.lock` before the code keeps the installed environment in its own image
layer, so a code change doesn't reinstall it.

## Inline environments

An inline manifest (`dependencies`, `pypi_dependencies`) is written in the pod and
solved there on each run. To reuse it, mount a volume and point `env_cache_path` at it:

```python
from kubernetes.client import models as k8s

PixiKubernetesPodOperator(
    task_id="summarize",
    pypi_dependencies=["pandas", "pyarrow"],
    env_cache_path="/cache/pixi",
    volumes=[k8s.V1Volume(name="cache", persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="pixi-cache"))],
    volume_mounts=[k8s.V1VolumeMount(name="cache", mount_path="/cache")],
    python_callable=summarize,
)
```

Without `platforms`, an inline manifest is solved for `linux-64` and `linux-aarch64`, the nodes a pod
can be scheduled on, whatever the platform of the worker that starts it. The solve fails if a package
is missing on either, as many bioconda packages are on `linux-aarch64`. Then list the platforms of
your nodes yourself, and keep the pod on them with a `node_selector`:

```python
PixiKubernetesPodOperator(
    task_id="align",
    dependencies={"bwa": "*", "samtools": "*"},
    channels=["conda-forge", "bioconda"],
    platforms=["linux-64"],
    node_selector={"kubernetes.io/arch": "amd64"},
    python_callable="align:run",
)
```

## What reaches the pod

The function's source, its arguments (`op_args`, `op_kwargs`), the context values described above and
an inline manifest travel in the pod's environment variables, base64-encoded. They are therefore part
of the pod's spec:

- Anyone who can read pods in the namespace sees them, for example with `kubectl get pod -o yaml`.
- `KubernetesPodOperator` writes the pod's spec into the task's error message when the pod fails,
  unless `log_pod_spec_on_failure=False`.

Don't pass secrets as arguments, `params` or `conf`. Give them to the pod as Kubernetes Secrets
instead, through `KubernetesPodOperator`'s `secrets` or `env_from`, and read them in the function
with `os.environ`.

Linux cannot start a process with an environment variable longer than 128 KiB, so the pod would
fail to start. The operator checks this before it creates the pod: when the encoded callable with its
arguments and context, or the inline manifest, is longer than 120 KiB
([`MAX_ENV_VALUE_BYTES`][airflow.providers.pixi.operators.kubernetes.MAX_ENV_VALUE_BYTES]), the task
fails with an error that names it:

```text
AirflowException: The callable with its op_args, op_kwargs and the task context needs 180 KiB in the
pod's environment variable PIXI_AIRFLOW_INPUT, more than the 120 KiB this operator allows, ...
```

Pass a path or URL instead of the data itself, or put the code into the image and use
`pixi_project_path` with a `"module.path:callable_name"` callable, whose source does not travel.

## Errors

The pod's log, with the traceback of an exception, is streamed to the task log. When the callable
raises, the task fails with
[`PixiCallableError`][airflow.providers.pixi.exceptions.PixiCallableError], as for the
[Pixi Operator](pixi.md#errors-skipping-and-timeouts):

```text
airflow.providers.pixi.exceptions.PixiCallableError: summarize raised FileNotFoundError: /data/input.parquet
```

The pod hands the exception back through its container's termination message, which Kubernetes keeps
up to 4 KiB of. The message is cut to 500 characters there, and the exception's `traceback` attribute
to its last lines. The whole traceback is in the task log. A failure before the callable runs, such
as an image that cannot be pulled or an environment that cannot be solved, fails the task with
`KubernetesPodOperator`'s own error.

`skip_on_exit_code` is `KubernetesPodOperator`'s: the exit codes of the container that skip the task.
The callable chooses one with `sys.exit(code)`. An exception exits with 1, so pick another code.

## How it works

The pod runs a short shell script that checks the pixi version, writes the inputs from the
environment variables to files, and runs `pixi run --manifest-path <manifest> python -c <runner>`,
with `--environment` and `--locked` or `--frozen` as for the Pixi Operator. The return value is
written to `/airflow/xcom/return.json`, where `KubernetesPodOperator`'s XCom sidecar picks it up. A
pickled return value travels as a base64 string in that file and is unpickled on the worker, both
when the task runs through and when it resumes after deferring.

## `@task.pixi_kubernetes`

```python
from airflow.sdk import task


@task.pixi_kubernetes(pypi_dependencies=["pandas", "pyarrow"], namespace="jobs")
def summarize(path: str, ds=None) -> dict:
    import pandas as pd

    return {"day": ds, **pd.read_parquet(path).describe().to_dict()}
```

It accepts every `PixiKubernetesPodOperator` argument. As for `@task.pixi`, the function must be
self-contained; see [Writing the function](../decorators/pixi.md#writing-the-function). Give context
parameters a default, as `ds=None` above.

## Testing in a cluster

`tests/system/pixi/test_example_kubernetes.py` runs the DAGs of `example_pixi_kubernetes.py` in real
pods through `dag.test()`: a JSON and a pickled return value, `do_xcom_push=False`, deferrable mode, and
an exception in the pod. CI runs it in a [kind](https://kind.sigs.k8s.io/) cluster
(`.github/workflows/kubernetes.yml`). To run it against your own cluster in the default kubeconfig:

```bash
export AIRFLOW_HOME=/tmp/airflow-k8s AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/tests/system/pixi
airflow db migrate
PIXI_K8S_E2E_TEST=1 pytest tests/system/pixi/test_example_kubernetes.py
```
