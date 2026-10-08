# Changelog

## 0.1.0

Initial version of the provider.

### Features

- `PixiOperator`: run a Python function, or a `"module.path:callable_name"` string, inside a Pixi
  environment defined by a project directory, a manifest file, or an inline manifest.
- `@task.pixi` TaskFlow decorator with the same arguments.
- Arguments and return values serialized as JSON or with pickle; `op_args` and `op_kwargs` are
  templated.
- Reuse of inline environments across runs with `env_cache_path`.
- Output of the run streamed to the task log; `execution_timeout` and killing the task stop pixi and
  every process it started.
- Automatic installation of the Pixi CLI when it is missing.
- Pixi, uv and pip cache directories configurable through Airflow Variables.
