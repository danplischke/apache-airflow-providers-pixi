"""Exceptions raised by the Pixi operators."""

from __future__ import annotations

from airflow.providers.pixi.utils.compat import AirflowException

__all__ = ("PixiCallableError",)


class PixiCallableError(AirflowException):
    """The Python callable raised an exception inside the Pixi environment.

    The message names the callable and the exception, such as ``train raised ValueError: no rows``; the
    traceback is in the task log, where the run printed it, and in :attr:`traceback`.

    :param callable_name: the function's name, or the ``"module.path:callable_name"`` string.
    :param error_type: the exception's class, with its module unless it is a built-in, such as ``ValueError``
        or ``requests.exceptions.HTTPError``.
    :param error_message: ``str()`` of the exception.
    :param traceback: the traceback, formatted inside the environment.
    """

    def __init__(self, callable_name: str, error_type: str, error_message: str, traceback: str) -> None:
        super().__init__(f"{callable_name} raised {error_type}: {error_message}")
        self.callable_name = callable_name
        self.error_type = error_type
        self.error_message = error_message
        self.traceback = traceback

    def __reduce__(self):
        return type(self), (self.callable_name, self.error_type, self.error_message, self.traceback)
