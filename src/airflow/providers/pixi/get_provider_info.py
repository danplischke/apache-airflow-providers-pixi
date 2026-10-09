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
                "python-modules": [
                    "airflow.providers.pixi.operators.pixi",
                    "airflow.providers.pixi.operators.bash",
                    "airflow.providers.pixi.operators.kubernetes",
                    "airflow.providers.pixi.operators.project_task",
                    "airflow.providers.pixi.operators.docker",
                    "airflow.providers.pixi.operators.external",
                ],
            }
        ],
        "sensors": [
            {
                "integration-name": "Pixi",
                "python-modules": ["airflow.providers.pixi.sensors.pixi"],
            }
        ],
        "hooks": [
            {
                "integration-name": "Pixi",
                "python-modules": ["airflow.providers.pixi.hooks.pixi"],
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.pixi.hooks.pixi.PixiHook",
                "hook-name": "Pixi",
                "connection-type": "pixi",
                "ui-field-behaviour": {
                    "hidden-fields": ["schema", "port", "extra"],
                    "relabeling": {"host": "Host", "login": "Username", "password": "Token or password"},
                    "placeholders": {
                        "host": "repo.prefix.dev, *.prefix.dev or artifactory.example.com",
                        "login": "only for basic_http and netrc",
                        "password": "the token, or the password for basic_http and netrc",
                    },
                },
                "conn-fields": {
                    "auth_type": {
                        "label": "Auth type",
                        "description": (
                            "auto: basic_http with a username, else bearer_token. bearer_token (prefix.dev), "
                            "conda_token (anaconda.org, quetz), basic_http (Artifactory, Nexus), netrc (PyPI indexes)."
                        ),
                        "schema": {
                            "type": ["string", "null"],
                            "default": "auto",
                            "enum": ["auto", "bearer_token", "conda_token", "basic_http", "netrc"],
                        },
                    }
                },
            }
        ],
        "task-decorators": [
            {"name": "pixi", "class-name": "airflow.providers.pixi.decorators.pixi.pixi_task"},
            {"name": "pixi_bash", "class-name": "airflow.providers.pixi.decorators.bash.pixi_bash_task"},
            {
                "name": "pixi_kubernetes",
                "class-name": "airflow.providers.pixi.decorators.kubernetes.pixi_kubernetes_task",
            },
            {"name": "pixi_sensor", "class-name": "airflow.providers.pixi.decorators.sensor.pixi_sensor_task"},
            {"name": "pixi_branch", "class-name": "airflow.providers.pixi.decorators.branch.pixi_branch_task"},
            {
                "name": "pixi_short_circuit",
                "class-name": "airflow.providers.pixi.decorators.short_circuit.pixi_short_circuit_task",
            },
            {"name": "pixi_docker", "class-name": "airflow.providers.pixi.decorators.docker.pixi_docker_task"},
            {"name": "pixi_external", "class-name": "airflow.providers.pixi.decorators.external.pixi_external_task"},
        ],
    }
