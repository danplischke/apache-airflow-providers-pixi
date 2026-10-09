"""Inline manifests are checked against pixi's manifest schema, vendored for MIN_PIXI_VERSION."""

from __future__ import annotations

import importlib.resources
import json
import sys
from typing import Any
from unittest.mock import patch

import pytest
from airflow.sdk import dag, task

from airflow.providers.pixi.operators.pixi import PixiOperator
from airflow.providers.pixi.utils.compat import AirflowException
from airflow.providers.pixi.utils.manifest import (
    SCHEMA_FILE,
    build_pixi_toml,
    pypi_dependencies_from_requirements,
)
from airflow.providers.pixi.utils.pixi import MIN_PIXI_VERSION

if sys.version_info >= (3, 11):
    import tomllib
else:
    tomllib = pytest.importorskip("tomli")

WORKSPACE = {"channels": ["conda-forge"], "platforms": ["linux-64"]}


def add(a, b=0):
    return a + b


def test_vendored_schema_is_the_one_of_min_pixi_version() -> None:
    schema = json.loads(importlib.resources.files("airflow.providers.pixi.utils").joinpath(SCHEMA_FILE).read_text())
    assert schema["$id"] == f"https://pixi.sh/v{MIN_PIXI_VERSION}/schema/manifest/schema.json"


@pytest.mark.parametrize(
    "options",
    [
        pytest.param({}, id="minimal"),
        pytest.param({"workspace_name": 'my "project"'}, id="workspace name"),
        pytest.param({"dependencies": {"python": ">=3.10", "numpy": "*"}}, id="dependencies"),
        pytest.param({"dependencies": ["python 3.12.*", "numpy>=2", "conda-forge::scipy"]}, id="matchspecs"),
        pytest.param(
            {"pypi_dependencies": {"torch": {"version": ">=2", "extras": ["cuda"]}, "pandas": ">=2.0"}},
            id="pypi dependencies",
        ),
        pytest.param(
            {
                "pypi_dependencies": pypi_dependencies_from_requirements(
                    [
                        "pandas>=2",
                        "six",
                        "requests[socks]==2.32",
                        "mylib @ git+https://github.com/org/mylib@v1.2",
                        "other @ git+ssh://git@github.com/org/other",
                        "wheel @ https://example.com/wheel-1.0-py3-none-any.whl",
                        "local @ file:///opt/local",
                    ]
                )
            },
            id="requirements",
        ),
        pytest.param(
            {
                "pypi_options": {
                    "index-url": "https://pypi.example/simple",
                    "extra-index-urls": ["https://pypi.org/simple"],
                    "index-strategy": "unsafe-best-match",
                }
            },
            id="pypi options",
        ),
        pytest.param({"environments": {"test": ["test"], "cuda": ["cuda"]}}, id="environments as lists"),
        pytest.param(
            {
                "feature": {
                    "gpu": {
                        "platforms": ["linux-64"],
                        "dependencies": {"cuda": "12.*"},
                        "pypi_dependencies": {"cupy": "*"},
                        "system-requirements": {"cuda": "12"},
                    },
                    "test": {"dependencies": ["pytest"]},
                },
                "environments": {"gpu": {"features": ["gpu"], "solve-group": "default"}, "test": ["test"]},
            },
            id="features",
        ),
    ],
)
def test_good_manifests_are_written(options: dict[str, Any]) -> None:
    manifest = tomllib.loads(build_pixi_toml(**WORKSPACE, **options))
    assert manifest["workspace"]["channels"] == ["conda-forge"]


def test_feature_keys_of_pixi_reach_the_manifest() -> None:
    feature = {"gpu": {"pypi_dependencies": {"cupy": "*"}, "system-requirements": {"cuda": "12"}}}
    manifest = tomllib.loads(build_pixi_toml(**WORKSPACE, feature=feature))
    assert manifest["feature"]["gpu"] == {"pypi-dependencies": {"cupy": "*"}, "system-requirements": {"cuda": "12"}}


@pytest.mark.parametrize(
    ("options", "where", "message"),
    [
        pytest.param(
            {"pypi_options": {"index_url": "https://pypi.example/simple"}},
            "pypi-options",
            "Additional properties are not allowed ('index_url' was unexpected)",
            id="snake_case pypi option",
        ),
        pytest.param(
            {"environments": {"gpu": {"feature": ["gpu"]}}},
            "environments.gpu",
            "Additional properties are not allowed ('feature' was unexpected)",
            id="environment key typo",
        ),
        pytest.param(
            {"environments": {"gpu": "gpu"}},
            "environments.gpu",
            "must be of type 'object' or 'array', not 'string'",
            id="environment as a string",
        ),
        pytest.param(
            {"dependencies": {"numpy": 2}},
            "dependencies.numpy",
            "must be of type 'string' or 'object', not 'integer'",
            id="version as a number",
        ),
        pytest.param(
            {"dependencies": {"ruamel.yaml": {"verison": "0.18"}}},
            'dependencies."ruamel.yaml"',
            "Additional properties are not allowed ('verison' was unexpected)",
            id="matchspec table typo",
        ),
        pytest.param(
            {"pypi_dependencies": {"mylib": {"git": "https://github.com/org/mylib", "revision": "v1"}}},
            "pypi-dependencies.mylib",
            "Additional properties are not allowed ('revision' was unexpected)",
            id="git requirement typo",
        ),
        pytest.param(
            {"feature": {"gpu": {"pypi-dependency": {"cupy": "*"}}}},
            "feature.gpu",
            "Additional properties are not allowed ('pypi-dependency' was unexpected)",
            id="feature key typo",
        ),
        pytest.param(
            {"feature": {"gpu": ["cuda"]}},
            "feature.gpu",
            "must be of type 'object', not 'array'",
            id="feature as a list",
        ),
        pytest.param({"channels": [""]}, "workspace.channels[0]", "must not be empty", id="empty channel"),
        pytest.param({"dependencies": {"": "*"}}, "dependencies", "the key '' is not allowed", id="empty name"),
    ],
)
def test_bad_manifests_name_the_key_and_the_problem(options: dict[str, Any], where: str, message: str) -> None:
    with pytest.raises(ValueError, match="invalid inline pixi manifest") as excinfo:
        build_pixi_toml(**{**WORKSPACE, **options})
    assert f"at {where}: {message}. " in str(excinfo.value)
    assert f"schema of pixi {MIN_PIXI_VERSION}; pass validate_manifest=False" in str(excinfo.value)


def test_the_message_leaves_out_the_value() -> None:
    with pytest.raises(ValueError, match="at pypi-options.index-url: must be of type 'string', not 'array'") as e:
        build_pixi_toml(**WORKSPACE, pypi_options={"index-url": ["https://user:s3cret@pypi.example/simple"]})
    assert "s3cret" not in str(e.value)


def test_validate_manifest_false_writes_keys_the_schema_does_not_know() -> None:
    toml = build_pixi_toml(**WORKSPACE, pypi_options={"new-option": True}, validate_manifest=False)
    assert tomllib.loads(toml)["pypi-options"] == {"new-option": True}


def test_a_typo_fails_when_the_dag_is_parsed() -> None:
    with pytest.raises(ValueError, match=r"at pypi-options: .*'index_url' was unexpected"):
        PixiOperator(
            task_id="t", python_callable=add, dependencies={"python": "3.12.*"}, pypi_options={"index_url": "x"}
        )


def test_a_typo_fails_when_a_task_pixi_is_added_to_the_dag() -> None:
    @dag
    def typo():
        task.pixi(dependencies={"python": "3.12.*"}, environments={"gpu": {"feature": ["gpu"]}})(add)(1)

    with pytest.raises(ValueError, match=r"at environments.gpu: .*'feature' was unexpected"):
        typo()


def test_validate_manifest_false_skips_the_check_at_parse_and_run_time() -> None:
    op = PixiOperator(
        task_id="t",
        python_callable=add,
        dependencies={"python": "3.12.*"},
        platforms=["linux-64"],
        pypi_options={"new-option": True},
        validate_manifest=False,
    )
    assert tomllib.loads(op.inline_manifest_toml())["pypi-options"] == {"new-option": True}


def test_the_manifest_is_checked_again_when_the_task_runs() -> None:
    op = PixiOperator(task_id="t", python_callable=add, dependencies={"python": "3.12.*"}, platforms=["linux-64"])
    op.pypi_options = {"index_url": "x"}
    with pytest.raises(ValueError, match=r"at pypi-options: .*'index_url' was unexpected"):
        op.inline_manifest_toml()


def test_templated_requirements_do_not_stop_the_parse_time_check() -> None:
    with pytest.raises(ValueError, match="at pypi-options"):
        PixiOperator(
            task_id="t",
            python_callable=add,
            requirements=["{{ params.requirement }}"],
            pypi_options={"index_url": "x"},
        )


def test_the_parse_time_check_does_not_need_the_platform_of_the_dag_processor() -> None:
    with patch(
        "airflow.providers.pixi.operators.pixi.local_platform", side_effect=AirflowException("no pixi platform")
    ):
        op = PixiOperator(task_id="t", python_callable=add, dependencies={"python": "3.12.*"})
        with pytest.raises(AirflowException, match="no pixi platform"):
            op.inline_manifest_toml()


def test_project_manifests_are_not_checked() -> None:
    op = PixiOperator(task_id="t", python_callable=add, pixi_project_path="/proj", pypi_options={"index_url": "x"})
    assert op.pixi_project_path == "/proj"


def test_a_typo_in_a_kubernetes_pod_manifest_fails_when_the_dag_is_parsed() -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    from airflow.providers.pixi.operators.kubernetes import PixiKubernetesPodOperator

    with pytest.raises(ValueError, match=r"at feature.gpu: .*'pypi-dependency' was unexpected"):
        PixiKubernetesPodOperator(
            task_id="t",
            python_callable=add,
            dependencies={"python": "3.12.*"},
            feature={"gpu": {"pypi-dependency": {"cupy": "*"}}},
        )
