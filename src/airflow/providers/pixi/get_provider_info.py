"""Provider metadata registered via the ``apache_airflow_provider`` entry point. Keep in sync with provider.yaml."""


def get_provider_info() -> dict[str, object]:
    return {
        "package-name": "apache-airflow-providers-pixi",
        "name": "Pixi",
        "description": "Run Python callables inside `Pixi <https://pixi.sh>`__ environments.",
        "integrations": [
            {
                "integration-name": "Pixi",
                "external-doc-url": "https://pixi.sh",
                "tags": ["software"],
            }
        ],
        "operators": [
            {
                "integration-name": "Pixi",
                "python-modules": ["airflow.providers.pixi.operators.pixi"],
            }
        ],
        "task-decorators": [
            {"name": "pixi", "class-name": "airflow.providers.pixi.decorators.pixi.pixi_task"},
        ],
    }
