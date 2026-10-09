# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

An Apache Airflow 3 provider (`apache-airflow-providers-pixi`) that runs task code inside Pixi-managed environments, on the worker, in a Kubernetes pod or in a Docker container. Supports Airflow ≥ 3.1.2 and Python 3.10–3.14; workers need pixi ≥ 0.81.0 on `PATH` (the provider never installs it).

## Commands

Tooling is uv + just (+ pixi for anything beyond unit tests). `just` lists all recipes.

- `just dev` — `uv sync --group dev --group docs` and install pre-commit hooks
- `just test` — unit tests (`tests/unit`) with a fake pixi; extra args go to pytest: `just test -k serializer`, `just test tests/unit/pixi/operators/test_pixi.py::test_name`
- `just chore` — ruff format + fix, then lint (ruff check, ruff format --check, mypy)
- `just check` — what CI runs: lint, unit tests, strict docs build, package build, smoke test
- `just test-integration` / `just test-system` / `just test-k8s` — real pixi (needs conda-forge access) / DAGs via `dag.test()` / current kubectl context. These are gated by `PIXI_INTEGRATION_TEST=1`, `PIXI_E2E_TEST=1`, `PIXI_K8S_E2E_TEST=1` and skip otherwise.
- `just test-compat [3.1.8 ...]` — all tests against Airflow releases with official constraints (defaults to the matrix in `.github/workflows/qa.yml`)
- `just test-lowest` — unit tests with every direct dependency at its lowest allowed version on Python 3.10
- `just smoke` — imports the package without dev deps or the `cncf.kubernetes` / `docker` extras
- `just docs` / `just docs-build` — Zensical docs (serve / strict build)
- `just standalone` — local Airflow at http://localhost:8080 with `dev/dags`, which run in the Pixi project `dev/project`

## Architecture

Code lives in `src/airflow/providers/pixi/` (namespace package under `airflow.providers`).

**Execution model.** A Python callable's *source* (not a pickle of the function) is shipped to the Pixi environment and executed by `runtime/runner.py`, which is run as `pixi run --manifest-path <m> python -c <runner source> <serializer> <input> <output> [json]`. Inputs (callable source or `module:name`, args, kwargs, JSON-safe context) go in an input file; the result comes back via an output file so stdout/stderr stay free for the task log. Callable exceptions are written to `<output>.error` (and to the Kubernetes termination log when `PIXI_AIRFLOW_TERMINATION_LOG` is set) and surface as `PixiCallableError`. Consequences:
- `runtime/` runs inside the Pixi environment, not on the worker: **stdlib only, Python ≥ 3.10, must pass `mypy --strict`** (mypy is configured only for this directory).
- `utils/source.py` strips task decorators (`@task.pixi`, `@setup`, the decorator named by `custom_operator_name`, the provider's `pixi_*_task` factories) by blanking their lines and padding the source so tracebacks keep DAG-file line numbers. Callables must be self-contained (no closures; enforced in `BasePixiPythonOperator.__init__`).

**Class hierarchy** (mostly in `operators/pixi.py`):
- `BasePixiOperator` — picks the environment from exactly one of `pixi_project_path`, `pixi_toml_path`, or an inline manifest (`dependencies`/`pypi_dependencies`/`channels`/`platforms`, rendered by `utils/manifest.py`). `local_manifest()` resolves/writes the manifest at execution time (relative paths are relative to the DAG file; inline manifests go to `env_cache_path/pixi-<hash>` or a temp dir). `pixi_run_command()` adds `--environment` / `--locked` / `--frozen`. Validation runs in `__init__` with `ValueError` unless the value contains Jinja, in which case it is re-checked at run time with `AirflowException`.
- `BasePixiPythonOperator` — adds `python_callable`, `op_args`/`op_kwargs`, `serializer` (`json`/`pickle`), `get_python_source()`, `callable_input()`.
- `PixiRunEnvMixin` — builds the subprocess env: strips worker Python vars (`PYTHONPATH`, `VIRTUAL_ENV`, …), sets `PYTHONNOUSERSITE=1`, resolves `env_from_variables`/`env_from_connections` (`utils/env.py`), and writes `pixi` connection credentials (`hooks/pixi.py`) to temp `RATTLER_AUTH_FILE`/`NETRC` files.
- `PixiSubprocessMixin` — `run_callable()`: runs `python_command()` (by default `pixi run ... python`) in its own process group, streams output to the log, kills the whole tree on timeout/`on_kill`, maps exit codes to skip/fail.
- Concrete: `PixiOperator`, `PixiBranchOperator`, `PixiShortCircuitOperator` (same file); `PixiSensor` (holds one manifest across pokes); `PixiBashOperator` (combines with standard `BashOperator`); `PixiProjectTaskOperator` (runs a manifest `[tasks]` entry; `operators/task.py` keeps the deprecated `PixiTaskOperator` name); `PixiExternalPythonOperator` (overrides `python_command()` to run `<workspace>/.pixi/envs/<env>/bin/python` directly, no pixi at run time).
- Container operators (`operators/container.py` → `BasePixiContainerOperator`): ship runner/input/manifest as base64 env vars into a `sh -c` script (`container_script()`, `container_env()`). `PixiKubernetesPodOperator` (combines with `KubernetesPodOperator`, returns via the XCom sidecar, errors via the termination message; `cncf.kubernetes` extra) and `PixiDockerOperator` (combines with `DockerOperator`, writes result or error to one file read back through `retrieve_output`/`pickling_library`; `docker` extra; Docker's `environment` is renamed `env_vars` because `environment` is the Pixi environment).
- `decorators/` wrap each operator with `task_decorator_factory`; `PixiDecoratedOperator` passes `op_args`/`op_kwargs` both directly and via `kwargs_to_upstream` so XComArg dependencies are not lost.

**Extension contract.** Other providers subclass these operators (see `docs/extending.md`, tested by `tests/unit/pixi/test_extending.py`). Public extension points — `get_python_source()`, `add_pypi_dependencies()`, `inline_manifest`, `env_vars`, `op_kwargs`, `custom_operator_name`, `local_manifest()`, `inline_manifest_toml()`, `pixi_run_command()`, `callable_input()` — are API; don't change their behaviour casually.

**Airflow version compatibility.** Import Airflow names whose location differs between 3.1 and later from `utils/compat.py`, not directly.

## Things that must stay in sync

- `provider.yaml` and `get_provider_info.py` must list the same operators, decorators and connection types; unit tests check they match, validate against Airflow's schema, and import every listed module. A broken provider info breaks `import airflow` for all users.
- The sdist ships `tests/`, so any repo file a test reads must be in `[tool.hatch.build.targets.sdist] only-include` (`test_sdist.py` checks this).
- `test_versions.py` cross-checks versions across `pyproject.toml`, `provider.yaml`, `docs/changelog.md` and the `qa.yml` matrix. The package version comes from git tags (hatch-vcs).
- New docs pages must be added to `nav` in `zensical.toml`. Docstrings are rendered by mkdocstrings and use its cross-reference syntax (`` [`Name`][dotted.path] ``); `just docs-build` is strict, so broken references fail.

## Conventions

- ruff, line length 120, target py310. Lazy top-level exports in `__init__.py` via `_EXPORTS`.
- Unit tests use the `fake_pixi` / `make_fake_pixi` fixtures in `tests/conftest.py` (pass `pixi_binary=fake_pixi.path`); the fake records each call's argv/cwd/env in `fake_pixi.calls` and runs the command with the test's Python. Every behaviour change needs a unit test; add an integration/system test if it depends on real pixi or Airflow behaviour.
- User-visible changes get an entry in `docs/changelog.md` under the upcoming version (start a new section if the top one is already released), and the relevant guide in `docs/` is updated.
- Releases: add `## <ver>` to the changelog, `just bump <ver>` (updates `provider.yaml`), merge, then `just release <ver>` tags `v<ver>` from a clean `main`.
- `main` is protected: PRs only, code-owner approval, signed commits.
