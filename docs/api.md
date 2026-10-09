# Python API

::: airflow.providers.pixi.operators.pixi
    options:
      members: [PixiOperator, PixiBranchOperator, PixiShortCircuitOperator, BasePixiPythonOperator, BasePixiOperator, PixiSubprocessMixin, PixiRunEnvMixin, WORKER_PYTHON_VARIABLES]
      toc_label: operators.pixi

::: airflow.providers.pixi.operators.bash
    options:
      members: [PixiBashOperator]
      toc_label: operators.bash

::: airflow.providers.pixi.operators.project_task
    options:
      members: [PixiProjectTaskOperator]
      toc_label: operators.project_task

::: airflow.providers.pixi.operators.kubernetes
    options:
      members: [PixiKubernetesPodOperator, DEFAULT_IMAGE, DEFAULT_POD_PLATFORMS, MAX_ENV_VALUE_BYTES]
      toc_label: operators.kubernetes

::: airflow.providers.pixi.operators.docker
    options:
      members: [PixiDockerOperator]
      toc_label: operators.docker

::: airflow.providers.pixi.operators.container
    options:
      members: [BasePixiContainerOperator, DEFAULT_IMAGE, DEFAULT_CONTAINER_PLATFORMS, MAX_ENV_VALUE_BYTES]
      toc_label: operators.container

::: airflow.providers.pixi.operators.external
    options:
      members: [PixiExternalPythonOperator, environment_prefix, environment_python]
      toc_label: operators.external

::: airflow.providers.pixi.sensors.pixi
    options:
      members: [PixiSensor]
      toc_label: sensors.pixi

::: airflow.providers.pixi.decorators.pixi
    options:
      members: [pixi_task, PixiDecoratedOperator, BasePixiDecoratedOperator]
      toc_label: decorators.pixi

::: airflow.providers.pixi.decorators.bash
    options:
      members: [pixi_bash_task]
      toc_label: decorators.bash

::: airflow.providers.pixi.decorators.kubernetes
    options:
      members: [pixi_kubernetes_task]
      toc_label: decorators.kubernetes

::: airflow.providers.pixi.decorators.docker
    options:
      members: [pixi_docker_task]
      toc_label: decorators.docker

::: airflow.providers.pixi.decorators.external
    options:
      members: [pixi_external_task]
      toc_label: decorators.external

::: airflow.providers.pixi.decorators.sensor
    options:
      members: [pixi_sensor_task]
      toc_label: decorators.sensor

::: airflow.providers.pixi.decorators.branch
    options:
      members: [pixi_branch_task]
      toc_label: decorators.branch

::: airflow.providers.pixi.decorators.short_circuit
    options:
      members: [pixi_short_circuit_task]
      toc_label: decorators.short_circuit

::: airflow.providers.pixi.hooks.pixi
    options:
      members: [PixiHook, PixiCredentials, pixi_auth_env, AUTH_TYPES]
      toc_label: hooks.pixi

::: airflow.providers.pixi.exceptions
    options:
      members: [PixiCallableError]
      toc_label: exceptions

::: airflow.providers.pixi.utils.pixi
    options:
      members: [MIN_PIXI_VERSION, local_platform]
      toc_label: utils.pixi

::: airflow.providers.pixi.utils.context
    options:
      members: [CONTEXT_KEYS, serializable_context]
      toc_label: utils.context

::: airflow.providers.pixi.utils.env
    options:
      members: [resolve_env, parse_connection_reference, CONNECTION_FIELDS]
      toc_label: utils.env

::: airflow.providers.pixi.utils.manifest
    options:
      members: [pypi_dependencies_table, conda_dependencies]
      toc_label: utils.manifest

::: airflow.providers.pixi.runtime.runner
    options:
      toc_label: runtime.runner
