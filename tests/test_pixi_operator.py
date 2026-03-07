"""Unit tests for PixiOperator: callable resolution, command building, execute (mocked)."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from airflow_providers_pixi.operators.pixi import (
    PixiOperator,
    _build_pixi_toml,
    _resolve_callable_ref,
)


def test_resolve_callable_ref_string_valid() -> None:
    assert _resolve_callable_ref("mymodule:my_func") == "mymodule:my_func"
    assert _resolve_callable_ref("pkg.sub:foo") == "pkg.sub:foo"


def test_resolve_callable_ref_string_invalid() -> None:
    with pytest.raises(ValueError, match="module.path:callable_name"):
        _resolve_callable_ref("nomodule")
    with pytest.raises(ValueError, match="module.path:callable_name"):
        _resolve_callable_ref("a:b:c")


def test_resolve_callable_ref_callable() -> None:
    def my_func() -> None:
        pass

    ref = _resolve_callable_ref(my_func)
    assert ":" in ref
    assert ref.endswith(":my_func")
    assert ref.split(":")[0].endswith("test_pixi_operator") or ref.startswith("__main__:")


def test_build_pixi_toml_minimal() -> None:
    toml = _build_pixi_toml(
        channels=["conda-forge"],
        platforms=["linux-64"],
    )
    assert "[workspace]" in toml
    assert 'channels = ["conda-forge"]' in toml
    assert 'platforms = ["linux-64"]' in toml


def test_build_pixi_toml_with_dependencies() -> None:
    toml = _build_pixi_toml(
        channels=["conda-forge"],
        platforms=["linux-64", "osx-64"],
        dependencies={"python": ">=3.10", "numpy": "*"},
    )
    assert "[dependencies]" in toml
    assert "python" in toml
    assert "numpy" in toml


def test_build_pixi_toml_with_pypi() -> None:
    toml = _build_pixi_toml(
        channels=["conda-forge"],
        platforms=["linux-64"],
        pypi_dependencies={"pandas": ">=2.0"},
    )
    assert "[pypi-dependencies]" in toml
    assert "pandas" in toml


def test_build_pixi_toml_with_environments() -> None:
    toml = _build_pixi_toml(
        channels=["conda-forge"],
        platforms=["linux-64"],
        environments={"test": ["test"], "cuda": ["cuda"]},
    )
    assert "[environments]" in toml
    assert "test" in toml
    assert "cuda" in toml


def test_operator_init_requires_one_manifest_source() -> None:
    with pytest.raises(ValueError, match="Exactly one of"):
        PixiOperator(
            task_id="t",
            pixi_project_path="/tmp",
            pixi_toml_path="/tmp/pixi.toml",
            python_callable="m:f",
        )
    with pytest.raises(ValueError, match="Exactly one of"):
        PixiOperator(
            task_id="t",
            python_callable="m:f",
        )
    with pytest.raises(ValueError, match="Exactly one of"):
        PixiOperator(
            task_id="t",
            pixi_project_path="/tmp",
            dependencies={"python": "3.10"},
            python_callable="m:f",
        )


def test_operator_init_project_path_sets_manifest_dir() -> None:
    op = PixiOperator(
        task_id="t",
        pixi_project_path="/some/project",
        python_callable="mymodule:my_func",
    )
    assert op._manifest_dir == os.path.abspath("/some/project")
    assert op._callable_ref == "mymodule:my_func"


def test_operator_init_toml_path_sets_manifest_dir() -> None:
    op = PixiOperator(
        task_id="t",
        pixi_toml_path="/repo/pixi.toml",
        python_callable="m:f",
    )
    assert op._manifest_dir == os.path.abspath("/repo")


def test_operator_init_inline_sets_manifest_dir_none() -> None:
    op = PixiOperator(
        task_id="t",
        dependencies={"python": "3.10"},
        pypi_dependencies={"pandas": "*"},
        channels=["conda-forge"],
        platforms=["linux-64"],
        python_callable="m:f",
    )
    assert op._manifest_dir is None


def test_operator_command_building_project_path() -> None:
    op = PixiOperator(
        task_id="t",
        pixi_project_path="/proj",
        python_callable="m:f",
    )
    assert op.pixi_binary == "pixi"
    assert op._manifest_dir == os.path.abspath("/proj")


def test_operator_command_building_with_environment() -> None:
    op = PixiOperator(
        task_id="t",
        pixi_project_path="/proj",
        python_callable="m:f",
        environment="cuda",
    )
    assert op.environment == "cuda"


@patch("airflow_providers_pixi.operators.pixi.subprocess.run")
def test_operator_execute_mock_subprocess(mock_run: MagicMock) -> None:
    mock_run.return_value = MagicMock(returncode=0, stdout='other\n{"x": 1}', stderr="")
    # Execute needs inline manifest (or real project path); use inline for test
    # Use inline so we don't depend on filesystem
    op_inline = PixiOperator(
        task_id="t2",
        dependencies={"python": "3.10"},
        channels=["conda-forge"],
        platforms=["linux-64"],
        python_callable="json:loads",
        op_args=['["hello"]'],
        auto_install_pixi=False,
    )
    ctx = {"ti": MagicMock()}
    with (
        patch("airflow_providers_pixi.operators.pixi.shutil.which", return_value="/usr/bin/pixi"),
        patch("airflow_providers_pixi.operators.pixi.subprocess.run") as m_run,
    ):
        m_run.return_value = MagicMock(
            returncode=0,
            stdout='\n["hello"]',
            stderr="",
        )
        result = op_inline.execute(ctx)
    assert result == ["hello"]
    ctx["ti"].xcom_push.assert_called_once()
    call_kw = ctx["ti"].xcom_push.call_args[1]
    assert call_kw["key"] == "return_value"
    assert call_kw["value"] == ["hello"]


@patch("airflow_providers_pixi.operators.pixi.subprocess.run")
def test_operator_execute_subprocess_failure(mock_run: MagicMock) -> None:
    from airflow.exceptions import AirflowException

    mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="pixi error")
    op = PixiOperator(
        task_id="t",
        dependencies={"python": "3.10"},
        channels=["conda-forge"],
        platforms=["linux-64"],
        python_callable="m:f",
        auto_install_pixi=False,
    )
    with (
        patch("airflow_providers_pixi.operators.pixi.shutil.which", return_value="/usr/bin/pixi"),
        pytest.raises(AirflowException, match="Pixi run failed"),
    ):
        op.execute({"ti": MagicMock()})


def test_operator_cache_dir_variables_in_env() -> None:
    """Cache dir variable params set PIXI/UV/PIP_CACHE_DIR in subprocess env."""
    from unittest.mock import patch

    op = PixiOperator(
        task_id="t",
        dependencies={"python": "3.10"},
        channels=["conda-forge"],
        platforms=["linux-64"],
        python_callable="json:loads",
        op_args=['["x"]'],
        pixi_cache_dir_variable="pixi_cache_dir",
        uv_cache_dir_variable="uv_cache_dir",
        pip_cache_dir_variable="pip_cache_dir",
        auto_install_pixi=False,
    )
    ctx = {"ti": MagicMock()}
    with (
        patch("airflow_providers_pixi.operators.pixi.shutil.which", return_value="/usr/bin/pixi"),
        patch("airflow_providers_pixi.operators.pixi.Variable.get") as m_get,
    ):
        m_get.side_effect = lambda key, default_var=None: {
            "pixi_cache_dir": "/shared/pixi_cache",
            "uv_cache_dir": "/shared/uv_cache",
            "pip_cache_dir": "/shared/pip_cache",
        }.get(key, default_var)
        with patch("airflow_providers_pixi.operators.pixi.subprocess.run") as m_run:
            m_run.return_value = MagicMock(returncode=0, stdout='"x"', stderr="")
            op.execute(ctx)
    call_kw = m_run.call_args[1]
    env = call_kw["env"]
    assert env["PIXI_CACHE_DIR"] == "/shared/pixi_cache"
    assert env["UV_CACHE_DIR"] == "/shared/uv_cache"
    assert env["PIP_CACHE_DIR"] == "/shared/pip_cache"


def test_ensure_pixi_available_uses_which_when_found() -> None:
    """When pixi is on PATH, _ensure_pixi_available returns it without installing."""
    from airflow_providers_pixi.operators.pixi import _ensure_pixi_available

    with patch("airflow_providers_pixi.operators.pixi.shutil.which") as m_which:
        m_which.return_value = "/usr/local/bin/pixi"
        path = _ensure_pixi_available("pixi", auto_install=True)
    assert path == "/usr/local/bin/pixi"
    m_which.assert_called_once_with("pixi")


def test_ensure_pixi_available_raises_when_not_found_and_no_auto_install() -> None:
    """When pixi not on PATH and auto_install=False, raises AirflowException."""
    from airflow.exceptions import AirflowException
    from airflow_providers_pixi.operators.pixi import _ensure_pixi_available

    with patch("airflow_providers_pixi.operators.pixi.shutil.which", return_value=None):
        with pytest.raises(AirflowException, match="not found on PATH"):
            _ensure_pixi_available("pixi", auto_install=False)
