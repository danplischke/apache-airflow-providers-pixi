# Pixi Sensor

Use the [`PixiSensor`][airflow.providers.pixi.sensors.pixi.PixiSensor] to wait for a condition that
Python code checks inside a [Pixi](https://pixi.sh) environment, for example with a client library that
is not installed on the worker. It works like Airflow's `PythonSensor`: the callable runs on each poke,
and the sensor succeeds once it returns a truthy value.

## Using the sensor

```python
from airflow.providers.pixi.sensors.pixi import PixiSensor


def landed(path: str) -> bool:
    import s3fs

    return s3fs.S3FileSystem().exists(path)


PixiSensor(
    task_id="wait_for_input",
    requirements=["s3fs"],
    python_callable=landed,
    op_args=["s3://bucket/{{ ds }}/input.parquet"],
    poke_interval=60,
    timeout=6 * 60 * 60,
)
```

It accepts every [Pixi Operator](../operators/pixi.md) argument for the callable and the environment,
and every `BaseSensorOperator` argument (`poke_interval`, `timeout`, `mode`, `soft_fail`,
`exponential_backoff`, ...).

## Returning an XCom

`PokeReturnValue` is an Airflow class, which the environment doesn't have. Return its fields as a dict
instead, and the sensor's XCom is `xcom_value`:

```python
def newest_file(prefix: str) -> dict:
    import s3fs

    files = s3fs.S3FileSystem().ls(prefix)
    return {"is_done": bool(files), "xcom_value": max(files) if files else None}
```

A dict with only the keys `is_done` and `xcom_value` is read that way; any other return value is
checked for truthiness.

## Poke and reschedule mode

In `mode="poke"`, the default, the environment is prepared once and every poke of the run uses it. In
`mode="reschedule"` each poke is a new task try, so an inline environment would be built again for
each one: pass `env_cache_path` to keep it between pokes.

## `@task.pixi_sensor`

```python
from airflow.sdk import task


@task.pixi_sensor(requirements=["s3fs"], poke_interval=60, mode="reschedule", env_cache_path="/var/cache/pixi-airflow")
def landed(path: str) -> bool:
    import s3fs

    return s3fs.S3FileSystem().exists(path)
```

As for `@task.sensor`, a function used several times in a DAG gets unique task ids. As for
`@task.pixi`, the function must be self-contained; see
[Writing the function](../decorators/pixi.md#writing-the-function).
