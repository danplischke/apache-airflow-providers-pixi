"""``@task.pixi_bash``: run the Bash command a function returns inside a Pixi environment."""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from typing import Any, ClassVar

from airflow.sdk.bases.decorator import DecoratedOperator, task_decorator_factory

from airflow.providers.pixi.operators.bash import PixiBashOperator
from airflow.providers.pixi.utils.compat import SET_DURING_EXECUTION, context_merge, determine_kwargs


class PixiBashDecoratedOperator(DecoratedOperator, PixiBashOperator):
    """``@task.pixi_bash``: the function runs on the worker and returns the command, which runs in Pixi."""

    template_fields: Sequence[str] = (*DecoratedOperator.template_fields, *PixiBashOperator.template_fields)
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
        **PixiBashOperator.template_fields_renderers,
    }
    custom_operator_name = "@task.pixi_bash"
    overwrite_rtif_after_execution = True

    def __init__(
        self,
        *,
        python_callable: Callable[..., Any],
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            bash_command=SET_DURING_EXECUTION,
            **kwargs,
        )

    def execute(self, context: Any) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)
        self.bash_command = self.python_callable(*self.op_args, **kwargs)
        if not isinstance(self.bash_command, str) or not self.bash_command.strip():
            raise TypeError("The returned value from the TaskFlow callable must be a non-empty string.")
        self.render_template_fields(context)
        return super().execute(context)


def pixi_bash_task(python_callable: Callable[..., Any] | None = None, **kwargs: Any):
    """``@task.pixi_bash``: run the Bash command the function returns inside a Pixi environment.

    Like ``@task.bash``: the function runs on the worker and returns the command, which is rendered as a
    template and then run with ``pixi run ... bash -c``. Accepts every ``PixiBashOperator`` argument.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=PixiBashDecoratedOperator,
        **kwargs,
    )
