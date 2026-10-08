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
- `requirements`: pip requirement strings added to an inline manifest's `[pypi-dependencies]`; an
  inline manifest with PyPI packages but no `python` gets the worker's Python version.
- `env_vars`: environment variables for the run, not templated.
- Requires pixi 0.81.0 or newer on the workers (`MIN_PIXI_VERSION`), checked before the first run
  with a binary. The provider never installs pixi.
- Pixi, uv and pip cache directories configurable through Airflow Variables.
- `PixiBashOperator` and `@task.pixi_bash`: run a Bash command with `pixi run ... bash -c`, like
  `BashOperator`.
- `PixiKubernetesPodOperator` and `@task.pixi_kubernetes`: run a Python callable in a Pixi environment
  in a Kubernetes pod, with the `cncf.kubernetes` extra.
- `PixiSensor` and `@task.pixi_sensor`: a sensor whose callable runs in a Pixi environment on each poke.
- `environment` is templated, so the environment can be chosen per run.

### Extending

- Other providers can build operators and task decorators on `PixiOperator` and `@task.pixi`, with
  the extension points of `@task.virtualenv`: `get_python_source()`, `op_kwargs`, `env_vars` and
  `requirements`, plus `inline_manifest`. The decorator line of a decorator built on `@task.pixi`
  (its `custom_operator_name`) is removed from the shipped source.
