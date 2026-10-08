"""Run a Bash command inside a Pixi environment."""

from __future__ import annotations

import shlex
from collections.abc import Sequence
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator

from airflow.providers.pixi.operators.pixi import BasePixiOperator
from airflow.providers.pixi.utils.pixi import resolve_pixi


class PixiBashOperator(BasePixiOperator, BashOperator):
    """Run a Bash command inside a Pixi environment, like ``BashOperator``.

    The command runs as ``pixi run --manifest-path <manifest> [--environment <env>] bash -c <command>``, so
    every part of it, pipes and ``&&`` included, sees the activated environment. Accepts every
    ``BashOperator`` argument (``bash_command``, ``env``, ``append_env``, ``cwd``, ``skip_on_exit_code``,
    ``output_processor``, ...) and every :class:`~airflow.providers.pixi.operators.pixi.BasePixiOperator`
    argument for the environment. ``cwd`` defaults to the manifest's directory. The last line of output
    is the task's XCom, as for ``BashOperator``.

    ``env`` replaces the worker's environment unless ``append_env=True``, as for ``BashOperator``; pixi
    needs at least ``HOME`` and ``PATH``, so pass ``append_env=True`` with ``env``.
    """

    template_fields: Sequence[str] = (*BashOperator.template_fields, *BasePixiOperator.template_fields)
    template_fields_renderers = BashOperator.template_fields_renderers
    template_ext: Sequence[str] = BashOperator.template_ext
    custom_operator_name = "PixiBash"

    def execute(self, context: Any) -> Any:
        pixi = resolve_pixi(self.pixi_binary)
        with self.local_manifest() as (manifest, manifest_dir):
            bash_command, cwd = self.bash_command, self.cwd
            self.bash_command = shlex.join([*self.pixi_run_command(pixi, manifest), "bash", "-c", str(bash_command)])
            self.cwd = cwd or manifest_dir
            try:
                return super().execute(context)
            finally:
                self.bash_command, self.cwd = bash_command, cwd
