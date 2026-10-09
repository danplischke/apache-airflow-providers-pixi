# Security

A Pixi task runs code with the permissions of the Airflow worker, like any other Python task. The
points below are specific to this provider.

## Installing Pixi

The provider never downloads or installs pixi: tasks use the binary installed on the workers (see
[Pixi binary](operators/pixi.md#pixi-binary)). Install it when you build the worker image, from a
source you trust and pinned to a version, rather than at run time.

## Dependencies resolved at run time

An inline manifest, or a project without a `pixi.lock`, is solved against its channels when the task
runs, so a new upstream release can change what a task executes. For reproducible and reviewable
environments, use a project directory with a committed `pixi.lock` (`pixi_project_path`), pin
versions, and restrict `channels` to sources you trust.

## Isolation from the worker

The environment's Python doesn't see the worker's packages: `PYTHONPATH`, `PYTHONHOME`,
`PYTHONUSERBASE` and `VIRTUAL_ENV` are left out of the run's environment and `PYTHONNOUSERSITE=1` is
set, unless the task sets them itself (see
[Isolation from the worker's Python](operators/pixi.md#isolation-from-the-workers-python)). It still
runs as the worker's user, with the worker's other environment variables, so it can read what the
worker can read.

## The `pickle` serializer

With `serializer="pickle"`, the worker unpickles the return value written by code running in the Pixi
environment, and unpickling can execute code. Only use it with environments and callables you trust
as much as the DAG itself. The default `json` serializer does not have this property.

## Arguments of pixi tasks

`PixiTaskOperator` passes each of `task_args` to pixi unchanged, but a task that declares `args` gets
their values substituted into its `cmd` without quoting, and pixi's task shell interprets the result.
A value from `dag_run.conf`, a Param set when triggering, or another outside source can then run
commands on the worker. See [Task arguments](operators/task.md#task-arguments).

## Cache directories

Cache directories shared through Airflow Variables (`PIXI_CACHE_DIR`, `UV_CACHE_DIR`,
`PIP_CACHE_DIR`) and `env_cache_path` hold packages that later runs install and execute. Make them
writable only by the users that run Airflow tasks.

## Credentials

Pass secrets to a run through Airflow Connections and Variables rather than DAG code:

- `env_from_connections` and `env_from_variables` read them on the worker when the task runs. The
  values never pass through templates and are not stored with the task, so they don't show up in
  the rendered templates of the UI. Passwords, connection URIs and extras are masked in the task log;
  a Variable is masked only if its key contains a sensitive word such as `password` or `secret`. See
  [From Variables and Connections](operators/pixi.md#from-variables-and-connections).
- `env_vars` is not templated, but its values are part of the DAG file, so keep secrets out of it.
- Channel and index credentials of `pixi_conn_id` are written to temporary files with mode `0600`,
  which `RATTLER_AUTH_FILE` and `NETRC` point at for the run, and removed afterwards. See
  [Pixi connection](connections/pixi.md).

The callable and everything it starts can read these environment variables and files while it runs,
and so can anything else running as the worker's user. Masking applies to the task log, including
what the callable prints; a value the callable writes anywhere else is not masked.

Credentials embedded in a manifest, such as an index URL with a password in `pypi_options`, end up in
the `pixi.toml` that is written to disk and kept in `env_cache_path`. Use a `pixi` connection with
`auth_type` `netrc` instead.
