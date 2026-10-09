# Building on the Pixi Operator

Other providers can build their own operators and task decorators on `PixiOperator` and `@task.pixi`,
to run something around the user's function inside the Pixi environment: lineage tracking,
credentials, logging, and so on. The extension points are the ones `@task.virtualenv` has, except
that packages are added with `add_pypi_dependencies()` instead of appending to `requirements`, so a
mixin written for `@task.virtualenv` ports to `@task.pixi` with that one change.

## Extension points

| Member | Use |
|---|---|
| [`get_python_source()`][airflow.providers.pixi.operators.pixi.BasePixiPythonOperator.get_python_source] | The source shipped to the environment for a function. Override it to append a wrapper that rebinds the function's name. |
| `python_callable.__name__` | The name the runner calls in the shipped namespace. |
| `op_args`, `op_kwargs` | Arguments of the call. Set them in `execute` to pass values computed at run time. |
| `env_vars` | Environment variables for the run. Not templated, so secrets set here are never rendered into the UI. |
| [`add_pypi_dependencies(*requirements)`][airflow.providers.pixi.operators.pixi.BasePixiOperator.add_pypi_dependencies] | Adds pip requirement strings, such as `"mytracker>=1"`, to the inline manifest's `[pypi-dependencies]`, whether the DAG gave `pypi_dependencies` as a dict or as pip strings. Raises `AirflowException` for a project or manifest file, and `ValueError` for a package already listed. |
| `pypi_dependencies` | The PyPI dependencies as the DAG gave them. `add_pypi_dependencies` assigns it a new value, so save it before and restore it afterwards. |
| [`inline_manifest`][airflow.providers.pixi.operators.pixi.BasePixiOperator.inline_manifest] | Whether `add_pypi_dependencies` can extend the environment. A project or manifest file has to bring the packages itself. |
| `custom_operator_name` | `@task.<name>` of your decorator. That decorator line is removed from the shipped source, like `@task.pixi`. A DAG that uses your factory function by its own name instead, as `@tracked_pixi_task(...)`, keeps that line in the source, which then fails in the environment with a `NameError`. Only this provider's functions (`pixi_task`, `pixi_kubernetes_task`, `pixi_sensor_task`, `pixi_branch_task`, `pixi_short_circuit_task`) are also removed by name. |

Set `op_kwargs` and `env_vars`, and call `add_pypi_dependencies`, in `execute` before calling
`super().execute`.

The same members exist on [`PixiSensor`](sensors/pixi.md) and, except `env_vars` (use
`KubernetesPodOperator`'s), on [`PixiKubernetesPodOperator`](operators/kubernetes.md), so a wrapper can
target the worker, a pod or a sensor. To run a Pixi environment some other way, subclass
[`BasePixiOperator`][airflow.providers.pixi.operators.pixi.BasePixiOperator], which chooses and
prepares the environment (`local_manifest()`, `inline_manifest_toml()`, `pixi_run_command()`), or
[`BasePixiPythonOperator`][airflow.providers.pixi.operators.pixi.BasePixiPythonOperator], which adds the
callable and its serialized input (`callable_input()`). Restore
`env_vars` and `pypi_dependencies` afterwards if you add to them, so a retry starts from the DAG's arguments.

## A wrapping task decorator

The runtime travels as source next to the user's function, so the environment needs neither Airflow
nor your provider, only the packages your runtime imports.

```python
from contextlib import contextmanager
from typing import Any

from airflow.providers.pixi.decorators.pixi import PixiDecoratedOperator
from airflow.sdk.bases.decorator import task_decorator_factory

RUNTIME = '''

def _tracked(fn, args, kwargs, label):
    import mytracker  # installed in the environment through add_pypi_dependencies

    with mytracker.span(label):
        return fn(*args, **kwargs)
'''


class TrackedPixiDecoratedOperator(PixiDecoratedOperator):
    custom_operator_name = "@task.tracked_pixi"
    _config: dict[str, Any] | None = None

    # BaseOperatorMeta expects the most-derived class to define __init__
    def __init__(self, *, label: str = "step", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.span_label = label

    def execute(self, context: Any) -> Any:
        env_vars, pypi_dependencies = self.env_vars, self.pypi_dependencies
        self._config = {"label": self.span_label}
        self.env_vars = {"MYTRACKER_TOKEN": get_token(), **(env_vars or {})}
        if self.inline_manifest:
            self.add_pypi_dependencies("mytracker>=1")
        try:
            return super().execute(context)
        finally:
            self.env_vars, self.pypi_dependencies, self._config = env_vars, pypi_dependencies, None

    def get_python_source(self) -> str:
        source = super().get_python_source()
        if self._config is None:
            return source
        name = self.python_callable.__name__
        return (
            source
            + RUNTIME
            + f"\n_user_fn = {name}\n\n\ndef {name}(*args, **kwargs):\n"
            + f"    return _tracked(_user_fn, args, kwargs, **{self._config!r})\n"
        )


def tracked_pixi_task(python_callable=None, multiple_outputs=None, **kwargs):
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=TrackedPixiDecoratedOperator,
        **kwargs,
    )
```

Register `tracked_pixi_task` under `task-decorators` in your `provider.yaml` to make it available as
`@task.tracked_pixi`.

## An operator that runs your own runtime

To run a function of your runtime instead of the user's, pass a placeholder function as
`python_callable` and return the generated source under its name. Arguments computed at run time go
into `op_kwargs`:

```python
def my_runtime_task(config: dict[str, Any]) -> str:
    """Placeholder; the operator ships generated source under this name."""
    raise NotImplementedError


class StartRunOperator(PixiOperator):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(python_callable=my_runtime_task, **kwargs)

    def get_python_source(self) -> str:
        return RUNTIME_SOURCE + "\n\ndef my_runtime_task(config):\n    return start_run(**config)\n"

    def execute(self, context: Any) -> Any:
        self.op_kwargs = {"config": {"reference": f"{context['dag'].dag_id}/{context['run_id']}"}}
        return super().execute(context)
```

## Porting a `@task.virtualenv` wrapper

A mixin that wraps `_PythonVirtualenvDecoratedOperator` through the members above works when it is put
in front of `PixiDecoratedOperator` instead. The one change is where it adds packages: a virtualenv
operator's `requirements` list becomes a call to `add_pypi_dependencies`. For example, lamindb-airflow's
`RemoteLaminDBStepMixin` runs as a `@task.lamindb_pixi` step:

```python
from contextlib import contextmanager

from airflow.providers.lamindb.utils.remote import (
    RemoteLaminDBStepMixin,
    ensure_lamindb_requirement,
    lamindb_virtualenv_env,
)
from airflow.providers.pixi.decorators.pixi import PixiDecoratedOperator


class LaminDBPixiDecoratedOperator(RemoteLaminDBStepMixin, PixiDecoratedOperator):
    custom_operator_name = "@task.lamindb_pixi"

    def __init__(self, *, lamindb_conn_id="lamindb_default", lamindb_instance=None,
                 lamindb_version=None, **kwargs):
        super().__init__(lamindb_conn_id=lamindb_conn_id, lamindb_instance=lamindb_instance, **kwargs)
        self.lamindb_version = lamindb_version

    @contextmanager
    def _lamindb_environment(self, remote):
        if self.inline_manifest:  # a project environment brings lamindb itself, like a pod image
            ensure_lamindb_requirement(self, remote, self.lamindb_version)
        with lamindb_virtualenv_env(self, remote):
            yield
```

Its helper `ensure_lamindb_requirement` reads the listed packages from `operator.requirements` and assigns
the list back. For a Pixi operator, it reads their names from
[`pypi_dependencies_table`][airflow.providers.pixi.utils.manifest.pypi_dependencies_table], which takes
either form of `pypi_dependencies`, and passes what it adds to `add_pypi_dependencies`:

```diff
 def ensure_lamindb_requirement(operator, remote, lamindb_version):
-    requirements = list(operator.requirements)
+    listed = list(pypi_dependencies_table(operator.pypi_dependencies))
+    requirements = list(listed)
     if not lamindb_requirement_lines(requirements):
         lamindb_version = lamindb_version or remote.instance_lamindb_version()
     add_lamindb_requirement(requirements, lamindb_version)
-    operator.requirements = requirements
+    operator.add_pypi_dependencies(*requirements[len(listed):])
```

Differences to keep in mind:

- `add_pypi_dependencies` only extends inline manifests. With `pixi_project_path` or `pixi_toml_path`,
  the packages must be in that environment; check `inline_manifest` before adding them.
- There is no `python_version`, `system_site_packages`, `index_urls` or `expect_airflow`. The Python
  version is a `dependencies` entry, indexes go into `[pypi-options]` of a project's `pixi.toml`,
  and the environment never contains Airflow unless the manifest lists it.

## Depending on this provider

Make the Pixi integration an optional extra of your provider, so installing it does not pull in this
package:

```toml
[project.optional-dependencies]
pixi = ["apache-airflow-providers-pixi"]
```

Import `airflow.providers.pixi` lazily, when your decorator is used, and raise an `ImportError` that
names the extra when it is missing:

```python
def tracked_pixi_task(python_callable=None, multiple_outputs=None, **kwargs):
    try:
        from airflow.providers.pixi.decorators.pixi import PixiDecoratedOperator  # noqa: F401
    except ImportError as e:
        raise ImportError(
            '@task.tracked_pixi requires apache-airflow-providers-pixi. '
            'Install with: pip install "my-provider[pixi]"'
        ) from e
    ...
```
