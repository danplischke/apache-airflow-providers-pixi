"""Unit tests for PixiKubernetesPodOperator and @task.pixi_kubernetes.

The pod script runs here with ``sh`` and a fake pixi, with the environment variables the pod would get;
``KubernetesPodOperator.execute`` itself is replaced, so no cluster is needed.
"""

from __future__ import annotations

import base64
import datetime
import json
import os
import pickle
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import tomlkit
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults
from airflow.sdk import DAG, task
from kubernetes.client import models as k8s

from airflow.providers.pixi.decorators.kubernetes import pixi_kubernetes_task
from airflow.providers.pixi.exceptions import PixiCallableError
from airflow.providers.pixi.operators.kubernetes import (
    DEFAULT_IMAGE,
    DEFAULT_POD_PLATFORMS,
    MAX_ENV_VALUE_BYTES,
    PixiKubernetesPodOperator,
    _env_var_list,
)
from airflow.providers.pixi.operators.pixi import BasePixiPythonOperator
from airflow.providers.pixi.utils.compat import AirflowException, AirflowSkipException
from airflow.providers.pixi.utils.pixi import MIN_PIXI_VERSION

INLINE = {"dependencies": {"python": "3.12.*"}}


def add(a, b=0):
    return a + b


def when(year: int):
    import datetime

    return datetime.date(year, 1, 2)


def make(fake_pixi, **kwargs) -> PixiKubernetesPodOperator:
    if "pixi_project_path" not in kwargs and "pixi_toml_path" not in kwargs:
        kwargs = {**INLINE, **kwargs}
    kwargs.setdefault("python_callable", add)
    return PixiKubernetesPodOperator(task_id="t", pixi_binary=str(fake_pixi.path), **kwargs)


def run_pod(
    op: PixiKubernetesPodOperator, tmp_path: Path, context=None, workdir: Path | None = None
) -> subprocess.CompletedProcess[str]:
    """Run the pod's script as the pod would, with its XCom directory and termination log under tmp_path.

    :param workdir: the image's working directory, where the script starts.
    """
    op.xcom_dir = str(tmp_path / "xcom")
    op.termination_message_path = str(tmp_path / "termination-log")
    env = {**os.environ, **{e.name: e.value for e in op.pod_env_vars(context)}}
    env.pop("PWD", None)
    return subprocess.run(
        ["sh", "-c", op.pod_script()], env=env, cwd=workdir, capture_output=True, text=True, check=False
    )


def xcom(tmp_path: Path):
    return json.loads((tmp_path / "xcom" / "return.json").read_text())


def test_defaults() -> None:
    op = PixiKubernetesPodOperator(task_id="t", python_callable=add, **INLINE)
    assert op.image == DEFAULT_IMAGE == f"ghcr.io/prefix-dev/pixi:{MIN_PIXI_VERSION}"
    assert {"op_args", "op_kwargs", "pypi_dependencies", "image", "env_vars"} <= set(op.template_fields)
    assert not {"cmds", "arguments"} & set(op.template_fields)


def test_pod_runs_the_function_in_an_inline_environment(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, op_args=[2], op_kwargs={"b": 3}, pypi_dependencies=["pandas"])
    proc = run_pod(op, tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert xcom(tmp_path) == 5
    call = fake_pixi.calls[-1]
    assert call["argv"][3] == "python"
    assert 'pandas = "*"' in Path(call["argv"][2]).read_text()


def test_pod_reuses_an_inline_environment_in_env_cache_path(fake_pixi, tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    op = make(fake_pixi, env_cache_path=str(cache), op_args=[1])
    assert run_pod(op, tmp_path).returncode == 0
    assert run_pod(op, tmp_path).returncode == 0
    manifests = {call["argv"][2] for call in fake_pixi.calls}
    assert len(manifests) == 1
    assert Path(manifests.pop()).parent.parent == cache


def test_pod_runs_in_a_project_of_the_image(fake_pixi, tmp_path: Path) -> None:
    project = tmp_path / "project with space"
    project.mkdir()
    (project / "helpers.py").write_text("def triple(x):\n    return 3 * x\n")
    proc = run_pod(
        make(fake_pixi, pixi_project_path=str(project), python_callable="helpers:triple", op_args=[4]), tmp_path
    )
    assert proc.returncode == 0, proc.stderr
    assert xcom(tmp_path) == 12
    assert fake_pixi.calls[-1]["argv"][2] == str(project)
    assert os.path.realpath(fake_pixi.calls[-1]["cwd"]) == os.path.realpath(project)


@pytest.mark.parametrize(("argument", "path"), [("pixi_project_path", "proj"), ("pixi_toml_path", "proj/pixi.toml")])
def test_relative_project_paths_start_at_the_image_working_directory(
    fake_pixi, tmp_path: Path, argument: str, path: str
) -> None:
    workdir = tmp_path / "app"
    (workdir / "proj").mkdir(parents=True)
    (workdir / "proj" / "pixi.toml").touch()
    (workdir / "proj" / "helpers.py").write_text("def triple(x):\n    return 3 * x\n")
    op = make(fake_pixi, python_callable="helpers:triple", op_args=[4], **{argument: path})
    proc = run_pod(op, tmp_path, workdir=workdir)
    assert proc.returncode == 0, proc.stderr
    assert xcom(tmp_path) == 12
    call = fake_pixi.calls[-1]
    assert os.path.realpath(call["argv"][2]) == os.path.realpath(workdir / path)
    assert os.path.realpath(call["cwd"]) == os.path.realpath(workdir / "proj")


def test_relative_env_cache_path_starts_at_the_image_working_directory(fake_pixi, tmp_path: Path) -> None:
    workdir = tmp_path / "app"
    workdir.mkdir()
    op = make(fake_pixi, env_cache_path="cache", op_args=[1])
    proc = run_pod(op, tmp_path, workdir=workdir)
    assert proc.returncode == 0, proc.stderr
    manifest = Path(os.path.realpath(fake_pixi.calls[-1]["argv"][2]))
    assert manifest.parent.parent == Path(os.path.realpath(workdir / "cache"))


def test_pod_rejects_an_image_with_an_old_pixi(make_fake_pixi, tmp_path: Path) -> None:
    proc = run_pod(make(make_fake_pixi("0.80.2"), op_args=[1]), tmp_path)
    assert proc.returncode == 1
    assert f"pixi 0.80.2 in the image is older than {MIN_PIXI_VERSION}" in proc.stderr


def test_pod_fails_when_the_function_does_not_return(fake_pixi, tmp_path: Path) -> None:
    proc = run_pod(make(fake_pixi, python_callable="sys:exit", op_args=[0]), tmp_path)
    assert proc.returncode == 1
    assert "exited without returning" in proc.stderr


def test_pod_without_xcom_push_writes_no_xcom(fake_pixi, tmp_path: Path) -> None:
    assert run_pod(make(fake_pixi, op_args=[1], do_xcom_push=False), tmp_path).returncode == 0
    assert not (tmp_path / "xcom").exists()


def test_execute_hands_the_script_to_the_pod_and_unpickles_the_result(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, python_callable=when, op_args=[2026], serializer="pickle", env_vars={"KEEP": "1"})

    def pod_execute(self, context):
        assert self.cmds[:2] == ["sh", "-c"]
        assert {"KEEP", "PIXI_AIRFLOW_INPUT", "PIXI_AIRFLOW_RUNNER", "PIXI_AIRFLOW_MANIFEST"} <= {
            e.name for e in self.env_vars
        }
        assert run_pod(self, tmp_path).returncode == 0
        return xcom(tmp_path)

    with patch.object(KubernetesPodOperator, "execute", pod_execute):
        assert op.execute({"ti": MagicMock()}) == datetime.date(2026, 1, 2)
    assert op.cmds == [] and op.arguments == []
    assert {e.name: e.value for e in _env_var_list(op.env_vars)} == {"KEEP": "1"}


@pytest.mark.parametrize(
    "env_vars",
    [
        {"KEEP": "1"},
        [k8s.V1EnvVar(name="KEEP", value="1")],
        (k8s.V1EnvVar(name="KEEP", value="1"),),
    ],
    ids=["dict", "list", "tuple"],
)
def test_env_vars_of_kubernetes_pod_operator_reach_the_pod(env_vars) -> None:
    assert [(e.name, e.value) for e in _env_var_list(env_vars)] == [("KEEP", "1")]


def test_no_env_vars_add_none() -> None:
    assert _env_var_list(None) == []
    assert _env_var_list({}) == []
    assert _env_var_list([]) == []


@pytest.mark.parametrize(
    ("env_vars", "given"),
    [("KEEP=1", "str"), (["KEEP=1"], "list of str"), ([k8s.V1EnvVar(name="A", value="1"), None], "list of NoneType")],
)
def test_other_env_vars_fail_before_the_pod_exists(fake_pixi, env_vars, given: str) -> None:
    op = make(fake_pixi, op_args=[1])
    op.env_vars = env_vars
    with (
        patch.object(KubernetesPodOperator, "execute") as pod_execute,
        pytest.raises(TypeError, match=f"a list of k8s.V1EnvVar, not {given}$"),
    ):
        op.execute({"ti": MagicMock()})
    pod_execute.assert_not_called()
    assert op.env_vars == env_vars
    with pytest.raises(TypeError) as excinfo:
        _env_var_list("SECRET=1")
    assert "SECRET" not in str(excinfo.value)


def test_xcom_goes_where_the_sidecar_mounts_its_volume() -> None:
    assert PixiKubernetesPodOperator.xcom_dir == PodDefaults.XCOM_MOUNT_PATH == "/airflow/xcom"


def test_pickled_xcom_is_a_base64_string(fake_pixi, tmp_path: Path) -> None:
    assert run_pod(make(fake_pixi, python_callable=when, op_args=[2026], serializer="pickle"), tmp_path).returncode == 0
    assert pickle.loads(base64.b64decode(xcom(tmp_path))) == datetime.date(2026, 1, 2)


def test_pypi_dependencies_cannot_extend_a_project_of_the_image(fake_pixi) -> None:
    op = make(fake_pixi, pixi_project_path="/app", python_callable="m:f")
    op.pypi_dependencies = ["pandas"]
    with pytest.raises(Exception, match="can only extend an inline manifest"):
        op.pod_script()


def test_task_pixi_kubernetes_ships_the_function_without_its_decorator(fake_pixi, tmp_path: Path) -> None:
    with DAG("d") as dag:

        @task.pixi_kubernetes(pixi_binary=str(fake_pixi.path), namespace="jobs", **INLINE)
        def double(x: int) -> int:
            return x * 2

        double(21)

    op = dag.get_task("double")
    assert type(op).__name__ == "PixiKubernetesDecoratedOperator"
    assert op.namespace == "jobs"
    assert "@task.pixi_kubernetes" not in op.get_python_source()
    assert run_pod(op, tmp_path).returncode == 0
    assert xcom(tmp_path) == 42


def test_task_pixi_kubernetes_ships_the_source_of_pixi_operator() -> None:
    op_class = pixi_kubernetes_task(**INLINE)(add).operator_class
    assert op_class.get_python_source is BasePixiPythonOperator.get_python_source


def test_pixi_kubernetes_task_imported_under_its_own_name_is_stripped(fake_pixi, tmp_path: Path) -> None:
    with DAG("d") as dag:

        @pixi_kubernetes_task(pixi_binary=str(fake_pixi.path), **INLINE)
        def fails_in_the_pod(x: int) -> int:
            # a comment, which must keep its line too
            raise ValueError("line marker of the pod function")

        fails_in_the_pod(1)

    op = dag.get_task("fails_in_the_pod")
    source = op.get_python_source()
    assert "pixi_kubernetes_task" not in source
    line = next(
        i
        for i, text in enumerate(Path(__file__).read_text().splitlines(), 1)
        if text.strip() == 'raise ValueError("line marker of the pod function")'
    )
    assert "line marker" in source.splitlines()[line - 1]
    assert run_pod(op, tmp_path).returncode == 1
    error = json.loads((tmp_path / "termination-log").read_text())["pixi_callable_error"]
    assert error["type"] == "ValueError"
    assert f'File "{__file__}", line {line}, in fails_in_the_pod' in error["traceback"]


def boom(rows):
    raise ValueError(f"no rows in {rows}")


def ds_and_run_id(x, ds=None, run_id=None):
    return [x, ds, run_id]


FAILED_POD = AirflowException("Pod pod-1 returned a failure.")


def pod_with_termination_message(message: str | None, container: str = "base") -> SimpleNamespace:
    """The parts of a finished pod's status KubernetesPodOperator.cleanup gets."""
    terminated = SimpleNamespace(exit_code=1, message=message)
    status = SimpleNamespace(name=container, state=SimpleNamespace(terminated=terminated))
    return SimpleNamespace(metadata=SimpleNamespace(name="pod-1"), status=SimpleNamespace(container_statuses=[status]))


@pytest.mark.parametrize("lock_mode", ["locked", "frozen"])
def test_lock_mode_is_passed_after_the_manifest(fake_pixi, tmp_path: Path, lock_mode: str) -> None:
    project = tmp_path / "project"
    project.mkdir()
    (project / "helpers.py").write_text("def triple(x):\n    return 3 * x\n")
    op = make(
        fake_pixi,
        pixi_project_path=str(project),
        python_callable="helpers:triple",
        op_args=[2],
        environment="prod",
        lock_mode=lock_mode,
    )
    proc = run_pod(op, tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert xcom(tmp_path) == 6
    assert fake_pixi.calls[-1]["argv"][3:6] == ["--environment", "prod", f"--{lock_mode}"]


def test_templated_lock_mode_of_an_inline_manifest_fails_before_the_pod_exists(fake_pixi) -> None:
    op = make(fake_pixi, lock_mode="{{ params.mode }}")
    op.lock_mode = "frozen"
    with (
        patch.object(KubernetesPodOperator, "execute") as pod_execute,
        pytest.raises(AirflowException, match="needs a pixi.lock"),
    ):
        op.execute({"ti": MagicMock()})
    pod_execute.assert_not_called()


def test_templated_environment_of_an_inline_manifest_fails_before_the_pod_exists(fake_pixi) -> None:
    op = make(fake_pixi, environment="{{ params.env }}")
    op.environment = "gpu"
    with (
        patch.object(KubernetesPodOperator, "execute") as pod_execute,
        pytest.raises(AirflowException, match="an inline manifest has only the default environment"),
    ):
        op.execute({"ti": MagicMock()})
    pod_execute.assert_not_called()


def test_inline_manifest_is_solved_for_linux_nodes(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, op_args=[1])
    assert op.default_platforms() == list(DEFAULT_POD_PLATFORMS) == ["linux-64", "linux-aarch64"]
    assert run_pod(op, tmp_path).returncode == 0
    manifest = tomlkit.parse(Path(fake_pixi.calls[-1]["argv"][2]).read_text())
    assert manifest["workspace"]["platforms"] == ["linux-64", "linux-aarch64"]


def test_explicit_platforms_win(fake_pixi) -> None:
    op = make(fake_pixi, platforms=["linux-64"])
    assert tomlkit.parse(op.inline_manifest_toml())["workspace"]["platforms"] == ["linux-64"]


def test_the_callable_gets_the_task_context(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, python_callable=ds_and_run_id, op_args=["x"])
    context = {"ti": MagicMock(), "ds": "2026-01-02", "run_id": "manual__1"}
    assert run_pod(op, tmp_path, context).returncode == 0
    assert xcom(tmp_path) == ["x", "2026-01-02", "manual__1"]


def test_execute_passes_the_context_to_the_pod(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, python_callable=ds_and_run_id, op_args=["x"])

    def pod_execute(self, context):
        spec = json.loads(base64.b64decode(next(e.value for e in self.env_vars if e.name == "PIXI_AIRFLOW_INPUT")))
        return spec["context"]

    with patch.object(KubernetesPodOperator, "execute", pod_execute):
        assert op.execute({"ti": MagicMock(), "ds": "2026-01-02"})["ds"] == "2026-01-02"


def test_an_exception_reaches_the_termination_message(fake_pixi, tmp_path: Path) -> None:
    proc = run_pod(make(fake_pixi, python_callable=boom, op_args=["sales"]), tmp_path)
    assert proc.returncode == 1
    assert "ValueError: no rows in sales" in proc.stderr
    assert not (tmp_path / "xcom").exists()
    message = json.loads((tmp_path / "termination-log").read_text())["pixi_callable_error"]
    assert message["type"] == "ValueError"
    assert message["message"] == "no rows in sales"
    assert "in boom" in message["traceback"]


def test_a_long_exception_is_cut_to_fit_the_termination_message(fake_pixi, tmp_path: Path) -> None:
    deep = "".join(f"def f{i}():\n    return f{i + 1}()\n" for i in range(80))
    deep += "def f80():\n    raise ValueError('é' * 600)\nf0()"
    proc = run_pod(make(fake_pixi, python_callable="builtins:exec", op_args=[deep, {}]), tmp_path)
    assert proc.returncode == 1
    data = (tmp_path / "termination-log").read_bytes()
    assert len(data) <= 4000
    message = json.loads(data)["pixi_callable_error"]
    assert message["message"] == "é" * 500
    assert len(message["traceback"]) < len(proc.stderr)
    assert message["traceback"].endswith("ValueError: " + "é" * 600 + "\n")
    assert "in f80" in message["traceback"]
    assert "in f0" not in message["traceback"]


def test_success_writes_no_termination_message(fake_pixi, tmp_path: Path) -> None:
    assert run_pod(make(fake_pixi, op_args=[1]), tmp_path).returncode == 0
    assert not (tmp_path / "termination-log").exists()


def test_a_failed_pod_raises_the_exception_of_the_callable(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, python_callable=boom, op_args=["sales"])
    assert run_pod(op, tmp_path).returncode == 1
    remote_pod = pod_with_termination_message((tmp_path / "termination-log").read_text())
    with (
        patch.object(KubernetesPodOperator, "cleanup", side_effect=FAILED_POD),
        pytest.raises(PixiCallableError, match="^boom raised ValueError: no rows in sales$") as e,
    ):
        op.cleanup(pod=remote_pod, remote_pod=remote_pod, xcom_result=None, context={})
    assert e.value.error_type == "ValueError"
    assert "in boom" in e.value.traceback
    assert "returned a failure" in str(e.value.__cause__)


@pytest.mark.parametrize("message", [None, "", "OOMKilled", '{"other": 1}', "[1]"])
def test_other_pod_failures_keep_the_error_of_kubernetes_pod_operator(fake_pixi, message) -> None:
    remote_pod = pod_with_termination_message(message)
    with (
        patch.object(KubernetesPodOperator, "cleanup", side_effect=FAILED_POD),
        pytest.raises(AirflowException, match="returned a failure") as e,
    ):
        make(fake_pixi).cleanup(pod=remote_pod, remote_pod=remote_pod)
    assert not isinstance(e.value, PixiCallableError)


def test_skip_on_exit_code_still_skips(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, python_callable=boom, op_args=["sales"])
    assert run_pod(op, tmp_path).returncode == 1
    remote_pod = pod_with_termination_message((tmp_path / "termination-log").read_text())
    with (
        patch.object(KubernetesPodOperator, "cleanup", side_effect=AirflowSkipException("exit code 1")),
        pytest.raises(AirflowSkipException),
    ):
        op.cleanup(pod=remote_pod, remote_pod=remote_pod)


def test_a_successful_cleanup_raises_nothing(fake_pixi) -> None:
    with patch.object(KubernetesPodOperator, "cleanup") as cleanup:
        make(fake_pixi).cleanup(pod="p", remote_pod="r", xcom_result=None, context={})
    cleanup.assert_called_once_with("p", "r", xcom_result=None, context={})


def test_deferrable_mode_unpickles_the_result(fake_pixi, tmp_path: Path) -> None:
    op = make(fake_pixi, python_callable=when, op_args=[2026], serializer="pickle", deferrable=True)
    assert run_pod(op, tmp_path).returncode == 0
    with patch.object(KubernetesPodOperator, "execute", return_value=None):
        assert op.execute({"ti": MagicMock()}) is None
    event = {"status": "success", "name": "pod-1", "namespace": "default"}
    with patch.object(KubernetesPodOperator, "trigger_reentry", return_value=xcom(tmp_path)) as reentry:
        assert op.trigger_reentry({"ti": MagicMock()}, event) == datetime.date(2026, 1, 2)
    assert reentry.call_args.args[1] is event


@pytest.mark.parametrize(
    ("serializer", "do_xcom_push", "result"),
    [("json", True, "aGk="), ("pickle", False, None), ("json", True, {"a": 1})],
)
def test_other_results_are_returned_as_they_are(fake_pixi, serializer, do_xcom_push, result) -> None:
    op = make(fake_pixi, serializer=serializer, do_xcom_push=do_xcom_push)
    with patch.object(KubernetesPodOperator, "trigger_reentry", return_value=result):
        assert op.trigger_reentry({}, {}) == result


def test_a_pickled_string_is_decoded_once(fake_pixi) -> None:
    op = make(fake_pixi, serializer="pickle")
    encoded = base64.b64encode(pickle.dumps(base64.b64encode(b"x").decode())).decode()
    with patch.object(KubernetesPodOperator, "execute", return_value=encoded):
        assert op.execute({"ti": MagicMock()}) == "eA=="


def test_too_large_arguments_fail_before_the_pod_exists(fake_pixi) -> None:
    op = make(fake_pixi, op_args=["x" * MAX_ENV_VALUE_BYTES])
    with (
        patch.object(KubernetesPodOperator, "execute") as pod_execute,
        pytest.raises(AirflowException, match="PIXI_AIRFLOW_INPUT") as e,
    ):
        op.execute({"ti": MagicMock()})
    pod_execute.assert_not_called()
    assert "The callable with its op_args" in str(e.value)
    assert "128 KiB" in str(e.value)
    assert "path or URL" in str(e.value)


def test_too_large_inline_manifest_fails_before_the_pod_exists(fake_pixi) -> None:
    op = make(fake_pixi, dependencies={f"package-{i:06d}": ">=1.0" for i in range(10000)})
    with pytest.raises(AirflowException, match="The inline manifest needs .* PIXI_AIRFLOW_MANIFEST"):
        op.pod_env_vars()


def test_arguments_under_the_limit_reach_the_pod(fake_pixi, tmp_path: Path) -> None:
    value = "x" * (MAX_ENV_VALUE_BYTES // 2)
    op = make(fake_pixi, python_callable="builtins:len", op_args=[value])
    assert all(len(e.value.encode()) <= MAX_ENV_VALUE_BYTES for e in op.pod_env_vars())
    assert run_pod(op, tmp_path).returncode == 0
    assert xcom(tmp_path) == len(value)


def test_task_pixi_kubernetes_fills_context_parameters(fake_pixi, tmp_path: Path) -> None:
    with DAG("d") as dag:

        @task.pixi_kubernetes(pixi_binary=str(fake_pixi.path), **INLINE)
        def step(x: int, ds=None):
            return [x, ds]

        step(1)

    assert run_pod(dag.get_task("step"), tmp_path, {"ds": "2026-01-02"}).returncode == 0
    assert xcom(tmp_path) == [1, "2026-01-02"]


async def add_later(a, b=0):
    import asyncio

    await asyncio.sleep(0)
    return a + b


def test_pod_awaits_an_async_function(fake_pixi, tmp_path: Path) -> None:
    proc = run_pod(make(fake_pixi, python_callable=add_later, op_args=[2], op_kwargs={"b": 3}), tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert xcom(tmp_path) == 5
