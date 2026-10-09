"""Smoke test of a base install: no dev dependencies and no cncf.kubernetes or docker extra.

Run by ``just smoke`` and by the smoke-test job in qa.yml. Airflow validates get_provider_info whenever it
is imported, so a broken one breaks ``import airflow`` for every installation that has this package.
"""

from __future__ import annotations

import importlib.util

from airflow.sdk import task

from airflow.providers.pixi.get_provider_info import get_provider_info

get_provider_info()
print("import and provider info OK")

for name in ("pixi", "pixi_bash", "pixi_sensor", "pixi_kubernetes", "pixi_docker", "pixi_external"):
    assert hasattr(task, name), f"@task.{name} is not registered"
print("task decorators registered")

if importlib.util.find_spec("airflow.providers.cncf") is None:
    try:
        task.pixi_kubernetes(lambda: None)
    except ImportError as e:
        assert "apache-airflow-providers-pixi[cncf.kubernetes]" in str(e), e
    else:
        raise SystemExit("@task.pixi_kubernetes worked without apache-airflow-providers-cncf-kubernetes")
    print("@task.pixi_kubernetes gives the install hint")
else:
    print("cncf.kubernetes is installed; install hint not checked")

if importlib.util.find_spec("airflow.providers.docker") is None:
    try:
        task.pixi_docker(lambda: None)
    except ImportError as e:
        assert "apache-airflow-providers-pixi[docker]" in str(e), e
    else:
        raise SystemExit("@task.pixi_docker worked without apache-airflow-providers-docker")
    print("@task.pixi_docker gives the install hint")
else:
    print("docker is installed; install hint not checked")
