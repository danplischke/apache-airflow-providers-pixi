# Changelog

## 0.1.0

Initial version of the provider.

### Requirements

- Apache Airflow 3.1.2 or newer; Airflow 3.0 is not supported. The provider floors,
  `apache-airflow-providers-standard>=1.9.1` and, with the `cncf.kubernetes` extra,
  `apache-airflow-providers-cncf-kubernetes>=10.9.0`, are the versions pinned by Airflow 3.1.2's
  constraints file, so every supported Airflow release can be installed with its own constraints file.
- `jsonschema>=4.19.1`, the floor of `apache-airflow-core`, for checking inline manifests.
- Pixi 0.81.0 or newer on the workers (`MIN_PIXI_VERSION`), checked before the first run with a
  binary. The provider never installs pixi.

### Operators, decorators and sensors

- `PixiOperator`: run a Python function, or a `"module.path:callable_name"` string, inside a Pixi
  environment defined by a project directory, a manifest file, or an inline manifest.
- `@task.pixi` TaskFlow decorator with the same arguments.
- `PixiBashOperator` and `@task.pixi_bash`: run a Bash command with `pixi run ... bash -c`, like
  `BashOperator`. `PixiBashOperator.pixi_command()` returns the command line, for subclasses.
- `PixiTaskOperator`: run a task from the manifest's `[tasks]` with `pixi run <task> [args]`. `task`
  and `task_args` are templated, and `task_args` can be another task's output. Arguments are
  shell-quoted for `bash -c`, so they reach pixi without word splitting or `$` expansion; pixi
  substitutes the values of a task's declared `args` into its `cmd` without quoting.
- `PixiKubernetesPodOperator` and `@task.pixi_kubernetes`: run a Python callable in a Pixi environment
  in a Kubernetes pod, with the `cncf.kubernetes` extra.
- `PixiSensor` and `@task.pixi_sensor`: a sensor whose callable runs in a Pixi environment on each poke.
- `PixiBranchOperator` and `@task.pixi_branch`: choose the tasks to follow with a callable run in a Pixi
  environment, like `BranchPythonVirtualenvOperator` and `@task.branch_virtualenv`.
- `PixiShortCircuitOperator` and `@task.pixi_short_circuit`: skip the tasks downstream when a callable
  run in a Pixi environment returns a falsy value, like `ShortCircuitOperator`; with
  `ignore_downstream_trigger_rules=False`, only the tasks directly downstream are skipped.

### Environments

- `environment` is templated, so the environment can be chosen per run.
- `lock_mode="locked"` or `"frozen"` passes `--locked` or `--frozen` to `pixi run`, so a project's
  `pixi.lock` is used as it is instead of being re-solved and rewritten. Without it, plain `pixi run`
  updates `pixi.lock` when the manifest changed. Inline manifests have no lock file and reject it.
- Inline manifests are solved for the platform of the machine running pixi (`local_platform()`:
  `linux-64`, `linux-aarch64`, `osx-64`, `osx-arm64` or `win-64`) instead of a fixed list, so packages
  that exist for only some platforms, such as many bioconda packages, can be used. `platforms` still
  overrides it. Pods default to `linux-64` and `linux-aarch64` (`DEFAULT_POD_PLATFORMS`).
- Relative `pixi_project_path`, `pixi_toml_path` and `env_cache_path` are resolved against the
  directory of the DAG file, after templates are rendered, so a project can live next to the DAG in a
  DAG bundle. In a pod, a relative path is relative to the image's working directory instead.
- `requirements`: pip requirement strings added to an inline manifest's `[pypi-dependencies]`; an
  inline manifest with PyPI packages but no `python` gets the worker's Python version.
- Reuse of inline environments across runs with `env_cache_path`.
- Inline manifests are checked against pixi's manifest schema for `MIN_PIXI_VERSION`, when the DAG is
  parsed and again when the task runs. A typo or a wrong shape raises `ValueError` that names the TOML
  key, such as `environments.gpu`, and leaves out the value, which can hold a password.
  `validate_manifest=False` turns the check off for keys only a newer pixi on the workers knows.
- `feature` tables take every key of a pixi feature, such as `system-requirements`, as pixi spells it.

### Calling the function

- Arguments and return values serialized as JSON or with pickle; `op_args` and `op_kwargs` are
  templated.
- The Airflow context reaches the function, as with `@task.virtualenv`: parameters named after a
  context key (`ds`, `run_id`, `params`, `logical_date`, `conf`, `ti`-derived values such as `dag_id`
  and `try_number`, and others in `CONTEXT_KEYS`) are filled when `op_args` and `op_kwargs` leave them
  unbound, and a `**kwargs` function gets all of them. Values cross as JSON; dates are ISO strings.
  This works for `"module:name"` callables, sensors and pods too.
- A function is shipped as source with the line numbers of the DAG file, so tracebacks point at it.
  The decorator line is removed, also when a decorator's factory, such as `pixi_task`, is imported and
  used by its own name.
- An exception raised by the function fails the task with `PixiCallableError`
  (`airflow.providers.pixi.exceptions`), whose message names the function, the exception type and its
  message, and which carries the traceback. A failure of pixi itself, before the function runs,
  reports pixi's exit code and the end of its output. In pods the error reaches the operator through
  the container's termination message.
- `skip_on_exit_code` on `PixiOperator`, `@task.pixi`, `PixiSensor` and `@task.pixi_sensor` skips the
  task on the given exit codes, as on `PythonVirtualenvOperator`.
- Output of the run streamed to the task log; `execution_timeout` and killing the task stop pixi and
  every process it started.

### Environment variables and credentials

- `env_vars`: environment variables for the run, not templated. `PixiBashOperator` and
  `@task.pixi_bash` accept it too, next to `BashOperator`'s `env`.
- `env_from_variables` and `env_from_connections` set environment variables from Airflow Variables
  and from Connections or one of their fields (`"conn_id.password"`, `"conn_id.extra.<key>"`, ...),
  resolved when the task runs. A missing Variable or Connection fails the task before pixi starts, and
  passwords, URIs and extra values are masked in the logs.
- New `pixi` connection type (`PixiHook`) for private conda channels and PyPI indexes, used with
  `pixi_conn_id`. The credentials are written to temporary files that pixi reads through
  `RATTLER_AUTH_FILE` (bearer token, conda token or basic HTTP) or `NETRC` (PyPI), merged with the
  credentials pixi would otherwise use, and removed when the run ends.
- Pixi, uv and pip cache directories configurable through Airflow Variables.
- `PixiBashOperator`, `@task.pixi_bash` and `PixiTaskOperator` support the cache directory Variables,
  `env_from_variables`, `env_from_connections` and `pixi_conn_id` like `PixiOperator`, including
  through `default_args`.

### Kubernetes pods

- With `deferrable=True` and `serializer="pickle"`, the XCom is the unpickled value, as in sync mode.
- The task fails before the pod is created when the encoded function, arguments and context, or the
  inline manifest, exceed 120 KiB (`MAX_ENV_VALUE_BYTES`), because Linux refuses environment variables
  over 128 KiB.
- `lock_mode` and the Airflow context are passed to the pod.
- `env_vars` takes a dict or a list of `V1EnvVar`. Anything else, such as a templated string without
  `render_template_as_native_obj`, fails the task with `TypeError` before the pod is created.

### Extending

- Other providers can build operators and task decorators on `PixiOperator` and `@task.pixi`, with
  the extension points of `@task.virtualenv`: `get_python_source()`, `op_kwargs`, `env_vars` and
  `requirements`, plus `inline_manifest`. The decorator line of a decorator built on `@task.pixi`
  (its `custom_operator_name`) is removed from the shipped source.
- `BasePixiOperator.pixi_run_options()` and `default_platforms()`, and `PixiRunEnvMixin`, which
  resolves the environment variables and credentials of a run for operators that start pixi
  themselves.

### Documentation and testing

- A [deployment guide](deployment.md): pixi in the worker image, shared caches, projects in DAG
  bundles, private channels, pods.
- CI runs the unit tests against the latest patch of each supported Airflow minor with its official
  constraints file, and with every direct dependency at its lowest allowed version.
- System tests through `dag.test()` for the Bash operator, the sensor in poke and reschedule mode, the
  context and readable errors, and for the pod operator in a [kind](https://kind.sigs.k8s.io/) cluster.
  Every DAG in `dev/dags` and `tests/system/pixi` is checked to import.
- Releases are published to PyPI only after every QA job passes on the release tag and the tag matches
  the package version.
