"""Example DAGs for PixiKubernetesPodOperator, run in a kind cluster by test_example_kubernetes.py."""

import datetime
from pathlib import Path

from airflow.sdk import DAG, task
from kubernetes.client import models as k8s

from airflow.providers.pixi.operators.kubernetes import PixiKubernetesPodOperator

CACHE = k8s.V1Volume(
    name="cache", host_path=k8s.V1HostPathVolumeSource(path="/var/tmp/pixi-airflow", type="DirectoryOrCreate")
)
POD = {
    "namespace": "default",
    "in_cluster": False,
    "startup_timeout_seconds": 600,
    "dependencies": {"python": "3.12.*"},
    "env_cache_path": "/cache/envs",
    "env_vars": {"PIXI_CACHE_DIR": "/cache/pixi"},
    "volumes": [CACHE],
    "volume_mounts": [k8s.V1VolumeMount(name="cache", mount_path="/cache")],
}


def stamp():
    import datetime

    return datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)


with DAG("pixi_kubernetes_example") as dag_ok:

    @task.pixi_kubernetes(**POD)
    def summarize(values: list, run_id=None) -> dict:
        import statistics
        import sys

        return {"mean": statistics.mean(values), "run_id": run_id, "python": list(sys.version_info[:2])}

    pickled = PixiKubernetesPodOperator(task_id="pickled", python_callable=stamp, serializer="pickle", **POD)
    deferred = PixiKubernetesPodOperator(
        task_id="deferred", python_callable=stamp, serializer="pickle", deferrable=True, poll_interval=5, **POD
    )
    no_xcom = PixiKubernetesPodOperator(
        task_id="no_xcom", python_callable="builtins:max", op_args=[1, 2], do_xcom_push=False, **POD
    )

    @task
    def check(summary: dict, pickled: datetime.datetime, deferred: datetime.datetime, run_id=None, ti=None) -> None:
        expected = stamp()
        assert summary == {"mean": 2, "run_id": run_id, "python": [3, 12]}, summary
        assert pickled == expected, pickled
        assert deferred == expected, deferred
        assert ti.xcom_pull(task_ids="no_xcom") is None

    no_xcom >> check(summarize([1, 2, 3]), pickled.output, deferred.output)


def record_exception(context) -> None:
    exception = context["exception"]
    out = Path(context["params"]["out"]) / f"{context['ti'].task_id}.txt"
    out.write_text(f"{type(exception).__name__}: {exception}")


def boom(rows: str) -> None:
    raise ValueError(f"no rows in {rows}")


with DAG("pixi_kubernetes_example_fail", params={"out": ""}) as dag_fail:
    failing_pod = {**POD, "python_callable": boom, "op_args": ["sales"], "on_failure_callback": record_exception}
    PixiKubernetesPodOperator(task_id="boom", **failing_pod)
    PixiKubernetesPodOperator(task_id="boom_deferred", deferrable=True, poll_interval=5, **failing_pod)
