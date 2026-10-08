"""@task.pixi_kubernetes against a local cluster, for `just standalone`.

Needs a cluster in ~/.kube/config, for example from kind, minikube or Docker Desktop. The pod pulls
ghcr.io/prefix-dev/pixi and solves the inline environment there, so it needs internet access.
"""

from __future__ import annotations

from airflow.sdk import DAG, task

with DAG("pixi_kubernetes_showcase", schedule=None, tags=["pixi", "kubernetes"]):

    @task.pixi_kubernetes(in_cluster=False, namespace="default", requirements=["numpy"])
    def numpy_in_a_pod() -> dict:
        import platform

        import numpy as np

        return {"numpy": np.__version__, "machine": platform.machine(), "mean": float(np.arange(10).mean())}

    numpy_in_a_pod()
