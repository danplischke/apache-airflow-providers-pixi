"""Inline Pixi manifests: ``pixi.toml`` written from operator arguments."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

import tomlkit
from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

_REQUIREMENT_COMMENT = re.compile(r"(^|\s)#.*$")
_MATCHSPEC = re.compile(r"^\s*(?:(?P<channel>[^:\s]+)::)?(?P<name>[A-Za-z0-9_.\-]+)\s*(?P<rest>.*?)\s*$")
_MATCHSPEC_JOINED = re.compile(r"\s*([,|])\s*")
_MATCHSPEC_OPERATOR = re.compile(r"([<>=!~]=?)\s+")


def _conda_spec(spec: str) -> tuple[str, Any]:
    match = _MATCHSPEC.match(spec)
    parts = _MATCHSPEC_OPERATOR.sub(r"\1", _MATCHSPEC_JOINED.sub(r"\1", match["rest"])).split() if match else []
    if match is None or len(parts) > 2 or any(c in match["rest"] for c in "[]:"):
        raise ValueError(
            f"invalid conda MatchSpec {spec!r}: write it as [channel::]name [version [build]], "
            "or use the dict form of dependencies for other keys"
        )
    version = parts[0] if parts else "*"
    table: dict[str, str] = {"version": version}
    if len(parts) == 2:
        table["build"] = parts[1]
    if match["channel"]:
        table["channel"] = match["channel"]
    return match["name"], table if len(table) > 1 else version


def conda_dependencies(dependencies: dict[str, Any] | Sequence[str] | None) -> dict[str, Any]:
    """``[dependencies]`` from a dict, or from a list of MatchSpecs such as ``"numpy>=2"``.

    A MatchSpec is ``[channel::]name [version [build]]``, such as ``"conda-forge::pytorch 2.* cuda*"``.

    :raises TypeError: for a string, which would otherwise be read one character at a time.
    :raises ValueError: for a MatchSpec of another form, or a package listed twice.
    """
    if isinstance(dependencies, str):
        raise TypeError(
            f"dependencies must be a dict or a list of MatchSpecs, such as [{dependencies!r}], not a string"
        )
    if dependencies is None or isinstance(dependencies, dict):
        return dict(dependencies or {})
    result: dict[str, Any] = {}
    for spec in dependencies:
        name, value = _conda_spec(spec)
        if any(listed.lower() == name.lower() for listed in result):
            raise ValueError(f"{name} is listed twice in dependencies")
        result[name] = value
    return result


def _pypi_spec_from_url(url: str) -> dict[str, str]:
    if url.startswith("git+"):
        repository = url[len("git+") :]
        at = repository.rfind("@")
        if at > repository.rfind("/"):
            return {"git": repository[:at], "rev": repository[at + 1 :]}
        return {"git": repository}
    if url.startswith("file://"):
        return {"path": url[len("file://") :]}
    return {"url": url}


def pypi_dependencies_table(pypi_dependencies: dict[str, Any] | Sequence[str] | str | None) -> dict[str, Any]:
    """``[pypi-dependencies]`` from a dict as in ``pixi.toml``, or from pip requirement strings.

    The strings are a list, or one string, as ``@task.virtualenv`` takes them. A string may hold several lines,
    such as a rendered requirements file; blank lines and comments are skipped. Versions, extras, git and URL
    requirements are supported.

    :raises ValueError: for a pip option, such as ``--index-url``, or an environment marker, which have no
        equivalent in an inline manifest and belong in a ``pixi.toml``; for an invalid requirement; and for a
        package listed twice.
    """
    if pypi_dependencies is None or isinstance(pypi_dependencies, dict):
        return dict(pypi_dependencies or {})
    result: dict[str, Any] = {}
    for item in [pypi_dependencies] if isinstance(pypi_dependencies, str) else pypi_dependencies:
        for raw in str(item).splitlines():
            line = _REQUIREMENT_COMMENT.sub("", raw).strip()
            if not line:
                continue
            if line.startswith("-"):
                raise ValueError(
                    f"pypi_dependencies cannot hold pip options such as {line!r}; set indexes and other PyPI "
                    "options under [pypi-options] in a pixi.toml and use pixi_project_path or pixi_toml_path"
                )
            try:
                requirement = Requirement(line)
            except InvalidRequirement as e:
                raise ValueError(f"invalid requirement {line!r}: {e}") from None
            if requirement.marker is not None:
                raise ValueError(f"requirement {line!r}: environment markers are not supported")
            if any(canonicalize_name(name) == canonicalize_name(requirement.name) for name in result):
                raise ValueError(f"{requirement.name} is listed twice in pypi_dependencies")
            spec: Any = str(requirement.specifier) or "*"
            if requirement.url:
                spec = _pypi_spec_from_url(requirement.url)
            if requirement.extras:
                spec = {**(spec if isinstance(spec, dict) else {"version": spec}), "extras": sorted(requirement.extras)}
            result[requirement.name] = spec
    return result


def build_pixi_toml(
    *,
    channels: Sequence[str],
    platforms: Sequence[str],
    dependencies: dict[str, Any] | Sequence[str] | None = None,
    pypi_dependencies: dict[str, Any] | Sequence[str] | str | None = None,
) -> str:
    """Return the ``pixi.toml`` of an inline manifest: ``[workspace]``, ``[dependencies]`` and ``[pypi-dependencies]``."""
    manifest: dict[str, Any] = {"workspace": {"channels": list(channels), "platforms": list(platforms)}}
    if dependencies:
        manifest["dependencies"] = conda_dependencies(dependencies)
    if pypi_table := pypi_dependencies_table(pypi_dependencies):
        manifest["pypi-dependencies"] = pypi_table
    return tomlkit.dumps(manifest)
