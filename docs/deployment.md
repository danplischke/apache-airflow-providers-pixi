# Deployment

This page covers what the Airflow workers need to run Pixi tasks in production: pixi itself, caches
shared between runs, projects shipped with the DAGs, and credentials for private channels. The
[Kubernetes pod operator](operators/kubernetes.md) runs pixi in a pod instead; its needs are at the
[end of this page](#kubernetes-pods).

## Installing pixi on the workers

The provider never downloads or installs pixi. Add it when you build the worker image, pinned to a
version (0.81.0 or newer), so every worker runs the same pixi. The simplest way is to copy the binary
from the [official pixi image](https://github.com/prefix-dev/pixi-docker):

```dockerfile
FROM apache/airflow:3.3.2-python3.12

COPY --from=ghcr.io/prefix-dev/pixi:0.81.0 /usr/local/bin/pixi /usr/local/bin/pixi
RUN pixi --version

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-pixi
```

The image's pixi is a static build, so it runs on any Linux base image, for `amd64` and `arm64`.
Copying it needs neither root nor a script downloaded at build time. To pin the exact binary, add the
image digest: `ghcr.io/prefix-dev/pixi:0.81.0@sha256:...`.

You can also run the [installer](https://pixi.sh/latest/installation/) as root, with
`PIXI_VERSION=v0.81.0 PIXI_BIN_DIR=/usr/local/bin PIXI_NO_PATH_UPDATE=1`. If pixi lives somewhere
other than `PATH`, pass its path as `pixi_binary`, for example through `default_args`. A task fails
with a clear error when pixi is missing or older than the minimum.

Upgrade pixi deliberately: a `pixi.lock` written by a newer pixi may not be readable by an older one,
so keep the workers' pixi at least as new as the one your team locks projects with.

## Shared caches

Pixi keeps downloaded packages in its cache directory and installs from there on later runs. By
default that is a directory in the home of the worker's user, so each worker, and each new
container, starts empty. To share it, point `PIXI_CACHE_DIR` at a volume every worker mounts, either
in the workers' environment:

```bash
PIXI_CACHE_DIR=/cache/pixi
```

or through an Airflow Variable, which can be changed without restarting the workers:

```bash
airflow variables set pixi_cache_dir /cache/pixi
```

```python
with DAG("pipeline", default_args={"pixi_cache_dir_variable": "pixi_cache_dir"}): ...
```

`uv_cache_dir_variable` and `pip_cache_dir_variable` do the same for `UV_CACHE_DIR` and
`PIP_CACHE_DIR`. See [Cache directories](operators/pixi.md#cache-directories). Tasks install and run
what the cache holds, so only the users that run Airflow tasks should be able to write to it.

## Inline environments on a volume

An inline manifest is built in a temporary directory and removed after each run. With
`env_cache_path`, it is kept in `<env_cache_path>/pixi-<hash of the manifest>` and reused by later
runs of the same manifest:

```python
with DAG("pipeline", default_args={"env_cache_path": "/cache/pixi-envs"}): ...
```

Concurrent runs of one manifest are safe. A changed manifest gets a new directory, and the provider
never removes old ones. Remove them when no task uses them, for example in a maintenance window:

```bash
find /cache/pixi-envs -mindepth 1 -maxdepth 1 -name 'pixi-*' -mtime +30 -exec rm -rf {} +
```

The modification time of a directory is when it was last changed, not when a task last used it, so
this can also remove environments still in use; the next run that needs one builds it again.

An inline manifest lists only the platform of the worker that runs it, such as `linux-64`. If workers
of several architectures share `env_cache_path`, pass `platforms` with all of them; see
[Platforms](operators/pixi.md#platforms).

## Projects in DAG bundles

A Pixi project can live next to the DAG that uses it, in the same
[DAG bundle](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html).
A relative `pixi_project_path` or `pixi_toml_path` is relative to the DAG file, wherever the worker
checked the bundle out:

```text
dags/
├── training.py
└── training-env/
    ├── pixi.toml
    └── pixi.lock
```

```python
PixiOperator(
    task_id="train",
    pixi_project_path="training-env",
    lock_mode="locked",
    python_callable="train:main",
)
```

Commit `pixi.lock`, with every platform your workers run on in `[workspace] platforms`, and set
`lock_mode`. Plain `pixi run` would rewrite `pixi.lock` in the checkout when it no longer matches the
manifest, which fails on a read-only bundle and runs versions nobody reviewed. `"locked"` fails
instead; `"frozen"` installs the lock file as it is. See [The lock file](operators/pixi.md#the-lock-file).

Pixi installs the environment into the project's `.pixi` directory, so the checkout must be writable,
and each new checkout of a bundle installs it again, from the cache if it is shared. For a read-only
checkout, set [`detached-environments`](https://pixi.sh/latest/reference/pixi_configuration/) in a
pixi configuration file of the workers, such as `/etc/pixi/config.toml`, to keep the environments
elsewhere:

```toml
detached-environments = "/cache/pixi-detached"
```

## Private channels and indexes

Store the credentials of private conda channels and PyPI indexes as connections of type `pixi`, and
pass their ids as `pixi_conn_id`. A PyPI index itself is named, without credentials, in the
`[pypi-options]` table of the project's `pixi.toml`; inline manifests take no index options.

```python
with DAG("pipeline", default_args={"pixi_conn_id": ["prefix_dev", "private_pypi"]}): ...
```

Each run gets a temporary credentials file, removed afterwards, so no credentials need to be baked
into the image or left on the workers. See [Pixi connection](connections/pixi.md). Other secrets the
callable needs, such as a database password, can reach it through `env_from_connections`; see
[From Variables and Connections](operators/pixi.md#from-variables-and-connections).

## Kubernetes pods

[`PixiKubernetesPodOperator`](operators/kubernetes.md) runs pixi in the pod, so the workers don't need
pixi, but the image does: pixi 0.81.0 or newer, `sh`, GNU `sort` and `base64`, and the project for
`pixi_project_path`. See [The image](operators/kubernetes.md#the-image). Paths are paths in the pod
(a relative one is relative to the image's working directory), caches and `env_cache_path` need a volume mounted in the pod, and `pixi_conn_id`, `env_from_variables`
and `env_from_connections` don't apply there: mount a Kubernetes Secret with pixi's credentials file
and set `RATTLER_AUTH_FILE` to its path in the pod's `env_vars`. Inline manifests are solved for
`linux-64` and `linux-aarch64` unless `platforms` says otherwise, and the encoded function, arguments
and context must stay under 120 KiB; see
[What reaches the pod](operators/kubernetes.md#what-reaches-the-pod).

## Security

Pixi tasks run with the permissions of the worker, and an environment solved at run time can change
when its channels publish new packages. See [Security](security.md) for installing pixi, the `pickle`
serializer, cache directories and credentials.
