"""Run a task defined in a Pixi manifest."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar

from airflow.providers.pixi.operators.bash import PixiBashOperator
from airflow.providers.pixi.operators.pixi import BasePixiOperator, PixiRunEnvMixin, _needs_rendering
from airflow.providers.pixi.utils.compat import SET_DURING_EXECUTION, AirflowException


class PixiTaskOperator(PixiBashOperator):
    """Run a task from the ``[tasks]`` of a Pixi manifest, as ``pixi run <task> [task_args...]`` does.

    The command is ``pixi run --manifest-path <manifest> [--environment <env>] [--locked | --frozen] <task>
    <task_args...>``, run through ``BashOperator``'s ``bash -c`` with each argument shell-quoted, so each one
    reaches pixi unchanged, without word splitting or ``$`` expansion by bash. Pixi appends them to the task's
    command when it declares no ``args``. A task that declares ``args`` gets their values substituted into its
    ``cmd`` without quoting, which pixi's task shell then interprets, so don't pass it values someone else
    controls, such as ``dag_run.conf``. A name that is not a task is run as a command of the environment, as
    ``pixi run`` does.

    Accepts every :class:`~airflow.providers.pixi.operators.bash.PixiBashOperator` argument except
    ``bash_command``: ``env``, ``append_env``, ``env_vars``, ``env_from_variables``, ``env_from_connections``,
    ``pixi_conn_id``, the cache directory Variables, ``skip_on_exit_code`` (default 99), ``output_processor``
    and ``cwd``, which defaults to the manifest's directory. The last line of output is the task's XCom. The
    environment comes from ``pixi_project_path`` or ``pixi_toml_path``: an inline manifest has no tasks.

    :param task: the name of the task (templated).
    :param task_args: arguments for the task, a list of strings (templated), or anything that resolves to one
        when the task runs, such as the output of another task.
    """

    template_fields: Sequence[str] = (
        "task",
        "task_args",
        "env",
        "cwd",
        *BasePixiOperator.template_fields,
        *PixiRunEnvMixin.template_fields,
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {"task_args": "json", "env": "json"}
    template_ext: Sequence[str] = ()
    custom_operator_name = "PixiTask"

    def __init__(self, *, task: str, task_args: Sequence[str] | None = None, **kwargs: Any) -> None:
        super().__init__(bash_command=SET_DURING_EXECUTION, **kwargs)
        if task is None or (isinstance(task, str) and not task.strip()):
            raise ValueError(f"task must be the name of a task of the manifest, not {task!r}")
        if isinstance(task_args, str) and not _needs_rendering(task_args):
            raise TypeError(f"task_args must be a list of arguments, such as [{task_args!r}], not a string")
        if self.inline_manifest:
            raise ValueError(
                "PixiTaskOperator runs a task of the manifest, which an inline manifest cannot define; "
                "use pixi_project_path or pixi_toml_path"
            )
        self.task = task
        self.task_args = list(task_args) if isinstance(task_args, (list, tuple)) else task_args or []

    def pixi_command(self, pixi: str, manifest: str) -> list[str]:
        """Return ``pixi run ... <task> <task_args...>``, from the rendered ``task`` and ``task_args``."""
        task = str(self.task).strip()
        if not task:
            raise AirflowException("task rendered to an empty name")
        if not isinstance(self.task_args, (list, tuple)):
            raise AirflowException(f"task_args must render to a list, not {type(self.task_args).__name__}")
        return [*self.pixi_run_command(pixi, manifest), task, *map(str, self.task_args)]
