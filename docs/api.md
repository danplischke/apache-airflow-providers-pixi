# Python API

::: airflow.providers.pixi.operators.pixi
    options:
      members: [PixiOperator, BasePixiPythonOperator, BasePixiOperator, PixiSubprocessMixin]
      toc_label: operators.pixi

::: airflow.providers.pixi.operators.bash
    options:
      members: [PixiBashOperator]
      toc_label: operators.bash

::: airflow.providers.pixi.operators.kubernetes
    options:
      members: [PixiKubernetesPodOperator, DEFAULT_IMAGE]
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

::: airflow.providers.pixi.utils.pixi
    options:
      members: [MIN_PIXI_VERSION]
      toc_label: utils.pixi
