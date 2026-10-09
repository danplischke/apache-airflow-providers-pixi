# Python API

::: airflow.providers.pixi.operators.pixi
    options:
      members: [PixiOperator, PixiBranchOperator, PixiShortCircuitOperator, BasePixiPythonOperator, BasePixiOperator, PixiSubprocessMixin, PixiRunEnvMixin]
      toc_label: operators.pixi

::: airflow.providers.pixi.operators.bash
    options:
      members: [PixiBashOperator]
      toc_label: operators.bash

::: airflow.providers.pixi.operators.task
    options:
      members: [PixiTaskOperator]
      toc_label: operators.task

::: airflow.providers.pixi.operators.kubernetes
    options:
      members: [PixiKubernetesPodOperator, DEFAULT_IMAGE, DEFAULT_POD_PLATFORMS, MAX_ENV_VALUE_BYTES]
      toc_label: operators.kubernetes

::: airflow.providers.pixi.sensors.pixi
    options:
      members: [PixiSensor]
      toc_label: sensors.pixi

::: airflow.providers.pixi.decorators.pixi
    options:
      members: [pixi_task, PixiDecoratedOperator]
      toc_label: decorators.pixi

::: airflow.providers.pixi.decorators.bash
    options:
      members: [pixi_bash_task]
      toc_label: decorators.bash

::: airflow.providers.pixi.decorators.kubernetes
    options:
      members: [pixi_kubernetes_task]
      toc_label: decorators.kubernetes

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
