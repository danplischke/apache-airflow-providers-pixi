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

## The `pickle` serializer

With `serializer="pickle"`, the worker unpickles the return value written by code running in the Pixi
environment, and unpickling can execute code. Only use it with environments and callables you trust
as much as the DAG itself. The default `json` serializer does not have this property.

## Cache directories

Cache directories shared through Airflow Variables (`PIXI_CACHE_DIR`, `UV_CACHE_DIR`,
`PIP_CACHE_DIR`) and `env_cache_path` hold packages that later runs install and execute. Make them
writable only by the users that run Airflow tasks.
