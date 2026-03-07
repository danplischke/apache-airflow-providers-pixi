def get_provider_info() -> dict[str, object]:
    return {
        "package-name": "apache-airflow-providers-pixi",
        "name": "Pixi",
        "description": "Run Python callables in Pixi envs (PixiOperator and @task.pixi).",
        "version": "0.1.0",
        "operators": [
            "airflow_providers_pixi.operators.pixi.PixiOperator",
        ],
        "task-decorators": [
            {
                "name": "pixi",
                "class-name": "airflow_providers_pixi.operators.pixi.pixi_task",
            },
        ],
    }
