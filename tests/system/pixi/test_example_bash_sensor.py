"""End-to-end tests of example_pixi_bash_sensor.py through Airflow's real task runner (``dag.test()``).

Covers PixiBashOperator, @task.pixi_bash, PixiSensor in poke mode, @task.pixi_sensor in reschedule mode with
env_cache_path, lock_mode, the Airflow context and Variables reaching a callable, and how a callable that raises
or exits with skip_on_exit_code ends its task. Same setup as test_example_dags.py::

    export AIRFLOW_HOME=/tmp/airflow-e2e AIRFLOW__CORE__LOAD_EXAMPLES=False
    export AIRFLOW__CORE__DAGS_FOLDER=$PWD/tests/system/pixi
    airflow db migrate
    PIXI_E2E_TEST=1 pytest tests/system/pixi
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("PIXI_E2E_TEST") != "1",
    reason="Set PIXI_E2E_TEST=1 (needs pixi on PATH and a migrated Airflow DB)",
)

DAG_FILE = Path(__file__).parent / "example_pixi_bash_sensor.py"
GREETING = "hello from a Variable"


@pytest.fixture(scope="module")
def env_cache(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("env_cache")


@pytest.fixture(scope="module")
def dags(env_cache: Path):
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("PIXI_E2E_ENV_CACHE", str(env_cache))
        mp.setenv("AIRFLOW_VAR_PIXI_E2E_GREETING", GREETING)
        spec = importlib.util.spec_from_file_location("unusual_prefix_e2e_example_pixi_bash_sensor", DAG_FILE)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        yield module


@pytest.fixture(scope="module")
def project(tmp_path_factory: pytest.TempPathFactory) -> Path:
    project = tmp_path_factory.mktemp("project")
    (project / "pixi.toml").write_text(
        '[workspace]\nname = "e2e-bash-sensor"\nchannels = ["conda-forge"]\n'
        'platforms = ["linux-64", "linux-aarch64", "osx-64", "osx-arm64"]\n\n[dependencies]\npython = "3.11.*"\n'
    )
    return project


@pytest.fixture
def out(tmp_path: Path) -> Path:
    out = tmp_path / "out"
    out.mkdir()
    return out


def _same_path(a: str | Path, b: str | Path) -> bool:
    return Path(a).resolve() == Path(b).resolve()


def _states(dr) -> dict[str, str]:
    return {ti.task_id: str(ti.state) for ti in dr.get_task_instances()}


def test_bash_operators_and_sensors(dags, project: Path, env_cache: Path, out: Path) -> None:
    dr = dags.dag_bash_sensor.test(run_conf={"project": str(project), "out": str(out)})
    assert str(dr.state) == "success", _states(dr)
    collected = json.loads((out / "collected.json").read_text())
    prefix = project / ".pixi" / "envs" / "default"

    greeting, bash_prefix = collected["bash"].split(" ", 1)
    assert greeting == "hello"
    assert _same_path(bash_prefix, prefix)
    assert (project / "pixi.lock").exists()

    word, ds, function_prefix = collected["bash_from_function"].split(" ", 2)
    assert (word, ds) == ("rendered", dr.logical_date.strftime("%Y-%m-%d"))
    assert _same_path(function_prefix, prefix)

    assert collected["poke"] == 2
    pokes = (out / "poke.txt").read_text().splitlines()
    assert len(pokes) == 2
    assert all(_same_path(p, prefix) for p in pokes)

    assert collected["reschedule"] == 2
    reschedules = (out / "reschedule.txt").read_text().splitlines()
    assert len(reschedules) == 2
    (env_dir,) = env_cache.glob("pixi-*")
    assert all(_same_path(p, env_dir / ".pixi" / "envs" / "default") for p in reschedules)


def test_callable_gets_the_context(dags, project: Path, out: Path) -> None:
    dr = dags.dag_context.test(run_conf={"project": str(project), "out": str(out)})
    assert str(dr.state) == "success", _states(dr)

    values = json.loads((out / "context.json").read_text())
    assert values["label"] == "from op_args"
    assert values["ds"] == dr.logical_date.strftime("%Y-%m-%d")
    assert values["run_id"] == dr.run_id
    assert values["params"] == {"project": str(project), "out": str(out)}
    assert datetime.fromisoformat(values["logical_date"]) == dr.logical_date
    assert values["greeting"] == GREETING


def test_exception_and_skip_on_exit_code(dags, project: Path, out: Path, capfd: pytest.CaptureFixture[str]) -> None:
    dr = dags.dag_callable_error.test(run_conf={"project": str(project), "out": str(out)})

    assert str(dr.state) == "failed"
    assert _states(dr) == {"raises": "failed", "skips": "skipped"}
    log = "".join(capfd.readouterr())
    assert "fail_on_purpose raised RuntimeError: input.csv is empty" in log
    assert 'raise RuntimeError(f"{path} is empty")' in log
