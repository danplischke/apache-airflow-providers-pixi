"""Airflow names whose import path differs between the Airflow versions this provider supports, 3.1 and newer.

``airflow.providers.common.compat.sdk`` covers most of them, but the version the Airflow 3.1.0 constraints pin
(1.7.4) has no ``sdk`` module and the one of 3.1.8 (1.14.0) has no ``determine_kwargs``, so the fallbacks are
kept here instead of raising the floor of that package above what users of Airflow 3.1 install.
"""

from __future__ import annotations

try:
    from airflow.sdk.exceptions import (
        AirflowException,
        AirflowSensorTimeout,
        AirflowSkipException,
        AirflowTaskTimeout,
    )
except ImportError:
    from airflow.exceptions import (  # type: ignore[no-redef]
        AirflowException,
        AirflowSensorTimeout,
        AirflowSkipException,
        AirflowTaskTimeout,
    )

try:
    from airflow.sdk.exceptions import AirflowNotFoundException
except ImportError:
    from airflow.exceptions import AirflowNotFoundException  # type: ignore[no-redef]

try:
    from airflow.sdk.bases.decorator import determine_kwargs
except ImportError:
    from airflow.utils.operator_helpers import determine_kwargs  # type: ignore[no-redef]

from airflow.sdk.definitions.context import context_merge

try:
    from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION
except ImportError:
    SET_DURING_EXECUTION = "DYNAMIC (set during execution)"  # type: ignore[assignment]

try:
    from airflow.sdk import BranchMixIn, SkipMixin
except ImportError:
    from airflow.providers.standard.operators.branch import BranchMixIn  # type: ignore[no-redef]
    from airflow.providers.standard.utils.skipmixin import SkipMixin  # type: ignore[no-redef]

__all__ = [
    "SET_DURING_EXECUTION",
    "AirflowException",
    "AirflowNotFoundException",
    "AirflowSensorTimeout",
    "AirflowSkipException",
    "AirflowTaskTimeout",
    "BranchMixIn",
    "SkipMixin",
    "context_merge",
    "determine_kwargs",
]
