# Pixi Docker Operator

Use the [`PixiDockerOperator`][airflow.providers.pixi.operators.docker.PixiDockerOperator] to run a
Python callable inside a [Pixi](https://pixi.sh) environment in a Docker container. It combines the
[Pixi Operator](pixi.md) with Airflow's `DockerOperator` and accepts the arguments of both, with these
exceptions:

- `command`, `entrypoint`, `retrieve_output` and `retrieve_output_path` are set by the operator,
  because they run the callable and bring back its result. Passing one fails when the DAG is parsed.
- `environment` is the Pixi environment to run in, as for every Pixi operator. The container's
  environment variables, `DockerOperator`'s `environment`, go in `env_vars`.

It needs the `docker` extra:

```bash
pip install "apache-airflow-providers-pixi[docker]"
```

## Using the operator

```python
from airflow.providers.pixi.operators.docker import PixiDockerOperator


def summarize(path: str) -> dict:
    import pandas as pd

    return pd.read_parquet(path).describe().to_dict()


PixiDockerOperator(
    task_id="summarize",
    docker_url="unix://var/run/docker.sock",
    pypi_dependencies=["pandas", "pyarrow"],
    python_callable=summarize,
    op_args=["/data/input.parquet"],
)
```

The callable works as for the [Pixi Operator](pixi.md#using-the-operator). It can be a self-contained
function, which is shipped as source, or a `"module.path:callable_name"` string, which is imported in
the container. Arguments and the return value travel as JSON, or with `serializer="pickle"`. The
return value is the task's XCom. It is copied out of the container after the container exits, so it
never passes through the log. Parameters named after context keys, such as `ds`, `params` or
`run_id`, get the task's values, as described in [Airflow context](pixi.md#airflow-context).

Everything else is `DockerOperator`'s: `docker_url` and `docker_conn_id` to reach the daemon and a
private registry, `mounts`, `network_mode`, `mem_limit`, `device_requests` for GPUs, `auto_remove`,
and so on. One difference: `DockerOperator` reads an `env_file` path ending in `.env` as a template
file, which would apply to `op_args` and `op_kwargs` too. Here `env_file` takes the file's content, so
read the file with `env_file="{% include 'app.env' %}"`.

## The image

The default image is `ghcr.io/prefix-dev/pixi:0.81.0`, the official pixi image in the minimum
supported version. The requirements for your own image are those of the
[Kubernetes pod operator](kubernetes.md#the-image):

- pixi 0.81.0 or newer, as `pixi` on `PATH` or at `pixi_binary`. The version is checked before the
  callable runs.
- `sh`, GNU `sort` and `base64`, as in Debian and Ubuntu based images. A writable `/tmp` for the
  result.
- for `pixi_project_path` or `pixi_toml_path`, the project, at that path in the image.

Paths (`pixi_project_path`, `pixi_toml_path`, `env_cache_path`) are paths in the container. A
relative path is relative to the image's working directory. Install the environment when the image is
built, so that runs don't install it again:

```dockerfile
FROM ghcr.io/prefix-dev/pixi:0.81.0

WORKDIR /app
COPY pixi.toml pixi.lock ./
RUN pixi install --locked
COPY . .
```

```python
PixiDockerOperator(
    task_id="train",
    image="registry.example.com/pipelines/train:1.4",
    pixi_project_path="/app",
    lock_mode="frozen",
    python_callable="jobs:train",
)
```

The operator replaces the image's `ENTRYPOINT` with `sh -c <script>`.

## Inline environments

An inline manifest (`dependencies`, `pypi_dependencies`) is written in the container and solved there
on each run. To reuse it, mount a volume and point `env_cache_path` at it:

```python
from docker.types import Mount

PixiDockerOperator(
    task_id="summarize",
    pypi_dependencies=["pandas", "pyarrow"],
    env_cache_path="/cache/pixi",
    mounts=[Mount(target="/cache", source="pixi-cache", type="volume")],
    python_callable=summarize,
)
```

Without `platforms`, an inline manifest is solved for `linux-64` and `linux-aarch64`, whatever the
platform of the worker that starts the container. If a package is missing on one of them, list the
platform of your Docker host with `platforms=["linux-64"]`.

## What reaches the container

The function's source, its arguments, the context values described above and an inline manifest
travel in the container's environment variables, base64-encoded. `DockerOperator` passes them as
`private_environment`, so they are not rendered in the UI. Anyone who can run `docker inspect` on the
host can still see them, so don't pass secrets as arguments, `params` or `conf`. Pass secrets in
`private_environment` instead and read them with `os.environ`.

As in a pod, each of these variables must stay under 120 KiB
([`MAX_ENV_VALUE_BYTES`][airflow.providers.pixi.operators.container.MAX_ENV_VALUE_BYTES]), because
Linux cannot start a process with a longer one. Larger values fail the task before the container is
created, with an error that names the variable. To stay under the limit, pass a path or URL instead
of the data, or put the code in the image and use a `"module.path:callable_name"` callable.

## Errors

The container's log, with the traceback of an exception, is streamed to the task log. When the
callable raises, the task fails with
[`PixiCallableError`][airflow.providers.pixi.exceptions.PixiCallableError], as for the
[Pixi Operator](pixi.md#errors-skipping-and-timeouts):

```text
airflow.providers.pixi.exceptions.PixiCallableError: summarize raised FileNotFoundError: /data/input.parquet
```

The exception travels back the same way as a return value, so its message and traceback are complete.
Other failures fail the task with `DockerOperator`'s own error. Examples are an image that cannot be
pulled, an environment that cannot be solved, or a pixi that is too old.

`skip_on_exit_code` lists the exit codes of `pixi run` that skip the task, as for the Pixi Operator.
The callable chooses one with `sys.exit(code)`, and an exception exits with 1. If the result cannot
be copied out of the container, the task fails rather than returning nothing.

## How it works

The container runs a short shell script. It checks the pixi version, writes the inputs from the
environment variables to files, and runs `pixi run --manifest-path <manifest> python -c <runner>`,
with `--environment` and `--locked` or `--frozen` as for the Pixi Operator. It then writes the return
value, or the exit code and the description of the exception, to `/tmp/pixi-airflow-result`, and
exits with 0. `DockerOperator`'s
`retrieve_output` copies that file out of the container before `auto_remove` removes the container.

## `@task.pixi_docker`

```python
from airflow.sdk import task


@task.pixi_docker(pypi_dependencies=["pandas", "pyarrow"], docker_url="tcp://docker:2375")
def summarize(path: str, ds=None) -> dict:
    import pandas as pd

    return {"day": ds, **pd.read_parquet(path).describe().to_dict()}
```

It accepts every `PixiDockerOperator` argument. As for `@task.pixi`, the function must be
self-contained; see [Writing the function](../decorators/pixi.md#writing-the-function). Give context
parameters a default, as `ds=None` above.
