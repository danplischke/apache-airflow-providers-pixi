# Pixi Branch and Short Circuit Operators

Two operators decide which tasks run next, with a Python callable that runs inside a
[Pixi](https://pixi.sh) environment:

- [`PixiBranchOperator`][airflow.providers.pixi.operators.pixi.PixiBranchOperator] and
  `@task.pixi_branch` choose the tasks to follow, like `BranchPythonVirtualenvOperator` and
  `@task.branch_virtualenv`.
- [`PixiShortCircuitOperator`][airflow.providers.pixi.operators.pixi.PixiShortCircuitOperator] and
  `@task.pixi_short_circuit` stop the pipeline when a condition is false, like `ShortCircuitOperator`
  and `@task.short_circuit`.

Both are [`PixiOperator`](pixi.md)s: they accept all of its arguments, and the callable is written
and run the same way. The environment comes from `pixi_project_path`, `pixi_toml_path` or an inline
manifest ([Choosing the environment](pixi.md#choosing-the-environment)); arguments and the return
value travel as JSON by default ([Arguments and return values](pixi.md#arguments-and-return-values));
parameters such as `params`, `ds` or `run_id` get the task's values
([Airflow context](pixi.md#airflow-context)); and `skip_on_exit_code`, `execution_timeout` and errors
of the callable work as described in [Errors, skipping and timeouts](pixi.md#errors-skipping-and-timeouts).
As for `@task.pixi`, a decorated function is shipped to the environment as source, so its imports
go inside it.

## Branching

The callable returns what to follow:

- a task id or a task group id directly downstream of the branch task, or a list of them. A task
  group id stands for the group's root tasks;
- `None`, to skip every task directly downstream.

The tasks directly downstream that are not followed are skipped, and the tasks after them are
skipped too unless their trigger rule says otherwise. A task id that is not in the DAG fails the
task, and so does a return value that is neither a string, a list of strings nor `None`.

```python
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task


@dag(params={"model": "small"})
def train():
    @task.pixi_branch(pixi_project_path="/repo/ml", environment="cpu")
    def choose(params) -> str:
        import torch

        if params["model"] == "large" and torch.cuda.is_available():
            return "train_large"
        return "train_small"

    join = EmptyOperator(task_id="report", trigger_rule="none_failed_min_one_success")
    train_large = EmptyOperator(task_id="train_large")
    train_small = EmptyOperator(task_id="train_small")
    choose() >> [train_large, train_small] >> join


train()
```

The same with the operator:

```python
from airflow.providers.pixi.operators.pixi import PixiBranchOperator


def choose(params) -> str:
    import torch

    if params["model"] == "large" and torch.cuda.is_available():
        return "train_large"
    return "train_small"


PixiBranchOperator(
    task_id="choose",
    python_callable=choose,
    pixi_project_path="/repo/ml",
    environment="cpu",
)
```

A task that comes after the followed branch is not skipped, even when it is also directly downstream
of the branch task: with `branch >> a >> join` and `branch >> join`, following `a` runs `join` too.
Give such a joining task a trigger rule such as `none_failed_min_one_success`, as `report` above,
because the default `all_success` skips it as soon as one of its upstream tasks was skipped.

## Short circuit

The callable returns a condition. A truthy value lets the pipeline continue; a falsy one, such as
`False`, `0`, `None`, `""` or an empty list, skips the tasks downstream. Teardown tasks are never
skipped.

```python
from airflow.sdk import dag, task


@dag
def nightly():
    @task.pixi_short_circuit(dependencies={"python": "3.12.*", "polars": "*"})
    def has_new_rows(ds) -> bool:
        import polars as pl

        return pl.scan_parquet(f"/data/events/{ds}/*.parquet").select(pl.len()).collect().item() > 0

    @task.pixi(dependencies={"python": "3.12.*", "polars": "*"})
    def aggregate(ds): ...

    has_new_rows() >> aggregate()


nightly()
```

`ignore_downstream_trigger_rules` decides how far the skip reaches:

- `True` (default): every task downstream is skipped, whatever its trigger rule.
- `False`: only the tasks directly downstream are skipped. The tasks after them run or are skipped
  as their trigger rules decide, so one with `trigger_rule="all_done"` still runs.

```python
from airflow.providers.pixi.operators.pixi import PixiShortCircuitOperator


def has_new_rows(ds) -> bool: ...


PixiShortCircuitOperator(
    task_id="has_new_rows",
    python_callable=has_new_rows,
    pixi_project_path="/repo/etl",
    ignore_downstream_trigger_rules=False,
)
```

## Return values

As for `BranchPythonOperator` and `ShortCircuitOperator`, the return value becomes the task's XCom only
when nothing is skipped: a branch choice that follows every task directly downstream, a truthy
condition, or a falsy one without tasks downstream. Either operator also pushes the tasks it
follows or skips as an XCom with the key `skipmixin_key`, which Airflow reads when those tasks are
cleared, so a cleared task that was skipped is skipped again.

## Decorators

`@task.pixi_branch` and `@task.pixi_short_circuit` take the same arguments as their operators, plus
`multiple_outputs`, as [`@task.pixi`](../decorators/pixi.md) does. The decorator line is removed from
the shipped source and line numbers are kept, so tracebacks point at the DAG file. Imported by name
from `airflow.providers.pixi.decorators.branch` (`pixi_branch_task`) and
`airflow.providers.pixi.decorators.short_circuit` (`pixi_short_circuit_task`), the decorators are
removed too.

## Reference

- [`PixiBranchOperator`][airflow.providers.pixi.operators.pixi.PixiBranchOperator]
- [`PixiShortCircuitOperator`][airflow.providers.pixi.operators.pixi.PixiShortCircuitOperator]
- [Branching](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/python.html#branchpythonvirtualenvoperator)
  and [ShortCircuitOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/python.html#shortcircuitoperator)
  in the standard provider's documentation
