"""Run a Bash command inside a Pixi environment."""

from __future__ import annotations

import contextlib
import shlex
from collections.abc import Sequence
from typing import Any

from airflow.providers.pixi.operators.pixi import BasePixiOperator, PixiRunEnvMixin
from airflow.providers.pixi.utils.compat import AirflowException
from airflow.providers.pixi.utils.pixi import resolve_pixi
from airflow.providers.standard.operators.bash import BashOperator


class PixiBashOperator(PixiRunEnvMixin, BasePixiOperator, BashOperator):
    """Run a Bash command inside a Pixi environment, like ``BashOperator``.

    The command runs as ``pixi run --manifest-path <manifest> [--environment <env>] [--locked | --frozen] bash -c
    <command>``, so every part of it, pipes and ``&&`` included, sees the activated environment. Accepts every
    ``BashOperator`` argument (``bash_command``, ``env``, ``append_env``, ``cwd``, ``skip_on_exit_code``,
    ``output_processor``, ...), every [`BasePixiOperator`][airflow.providers.pixi.operators.pixi.BasePixiOperator]
    argument for the environment, and the [`PixiRunEnvMixin`][airflow.providers.pixi.operators.pixi.PixiRunEnvMixin]
    arguments ``env_from_variables``, ``env_from_connections``, ``pixi_conn_id`` and the cache directory Variables, as
    ``PixiOperator`` does. ``cwd`` defaults to the manifest's directory. The last line of output is the task's XCom, as
    for ``BashOperator``.

    :param env_vars: environment variables for the run, on top of all others, as for ``PixiOperator``. Not
        templated, so secrets set here are never rendered into the UI.

    The environment of the run, in increasing precedence: the worker's environment, unless ``env`` is set
    without ``append_env=True``; the cache directory Variables; ``env_from_variables`` and
    ``env_from_connections``; ``env``; ``env_vars``. ``env`` replaces the worker's environment unless
    ``append_env=True``, as for ``BashOperator``; pixi needs at least ``HOME`` and ``PATH``, so pass
    ``append_env=True`` with ``env``.
    """

    template_fields: Sequence[str] = (
        *BashOperator.template_fields,
        *BasePixiOperator.template_fields,
        *PixiRunEnvMixin.template_fields,
    )
    template_fields_renderers = BashOperator.template_fields_renderers
    template_ext: Sequence[str] = BashOperator.template_ext
    custom_operator_name = "PixiBash"

    def __init__(self, *, env_vars: dict[str, str] | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.env_vars = env_vars
        self._env_stack: contextlib.ExitStack | None = None

    def pixi_command(self, pixi: str, manifest: str) -> list[str]:
        """Return the command that runs in place of ``bash_command``: ``pixi run ... bash -c <bash_command>``.

        Called when the task runs, as templated fields are then rendered. Override it to run something else
        through pixi, as [`PixiProjectTaskOperator`][airflow.providers.pixi.operators.project_task.PixiProjectTaskOperator] does.
        """
        return [*self.pixi_run_command(pixi, manifest), "bash", "-c", str(self.bash_command)]

    @classmethod
    def _is_inline_command(cls, bash_command: str) -> bool:
        return True

    def execute(self, context: Any) -> Any:
        pixi = resolve_pixi(self.pixi_binary)
        with self.local_manifest() as (manifest, manifest_dir), contextlib.ExitStack() as stack:
            bash_command, cwd, is_inline = self.bash_command, self.cwd, self._is_inline_cmd
            self.bash_command = shlex.join(self.pixi_command(pixi, manifest))
            self.cwd = cwd or manifest_dir
            self._is_inline_cmd = True
            self._env_stack = stack
            try:
                return super().execute(context)
            finally:
                self.bash_command, self.cwd, self._is_inline_cmd, self._env_stack = bash_command, cwd, is_inline, None

    def get_env(self, context: Any) -> dict[str, str]:
        """Return the environment of the run: ``BashOperator``'s, with the variables of this operator merged in.

        Called by ``execute``; see the class docstring for the precedence. Files written for ``pixi_conn_id``
        exist until ``execute`` returns.
        """
        if self._env_stack is None:
            raise AirflowException(f"{type(self).__name__}.get_env can only be called from execute")
        env = super().get_env(context)
        overrides = {**(self.env or {}), **(self.env_vars or {})}
        return self._env_stack.enter_context(self.pixi_run_env(env, overrides))
