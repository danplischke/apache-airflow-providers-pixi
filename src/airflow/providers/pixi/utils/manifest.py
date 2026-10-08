"""Inline Pixi manifests: ``pixi.toml`` written from operator arguments."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

import tomlkit
from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

_REQUIREMENT_COMMENT = re.compile(r"(^|\s)#.*$")
# a conda MatchSpec: name, optionally with a channel (``conda-forge::numpy``), then the version
_MATCHSPEC = re.compile(r"^\s*(?:(?P<channel>[^:\s]+)::)?(?P<name>[A-Za-z0-9_.\-]+)\s*(?P<version>.*?)\s*$")


def conda_dependencies(dependencies: dict[str, Any] | Sequence[str] | None) -> dict[str, Any]:
    """``[dependencies]`` from a dict, or from a list of MatchSpecs such as ``"numpy>=2"``."""
    if dependencies is None or isinstance(dependencies, dict):
        return dict(dependencies or {})
    result: dict[str, Any] = {}
    for spec in dependencies:
        match = _MATCHSPEC.match(spec)
        if match is None:
            raise ValueError(f"invalid conda MatchSpec {spec!r}")
        version = match["version"] or "*"
        result[match["name"]] = {"version": version, "channel": match["channel"]} if match["channel"] else version
    return result


def _pypi_spec_from_url(url: str) -> dict[str, str]:
    if url.startswith("git+"):
        repository = url[len("git+") :]
        # a revision follows the last "@" of the path, not the one of ``ssh://git@host``
        at = repository.rfind("@")
        if at > repository.rfind("/"):
            return {"git": repository[:at], "rev": repository[at + 1 :]}
        return {"git": repository}
    if url.startswith("file://"):
        return {"path": url[len("file://") :]}
    return {"url": url}


def pypi_dependencies_from_requirements(requirements: Sequence[str]) -> dict[str, Any]:
    """``[pypi-dependencies]`` for pip requirement lines, as ``@task.virtualenv`` takes them.

    An element may hold several lines, such as a rendered requirements file; blank lines and comments are
    skipped. Pip options and environment markers have no equivalent in an inline manifest.
    """
    result: dict[str, Any] = {}
    for item in requirements:
        for raw in str(item).splitlines():
            line = _REQUIREMENT_COMMENT.sub("", raw).strip()
            if not line:
                continue
            if line.startswith("-"):
                raise ValueError(f"requirements cannot hold pip options such as {line!r}; use pypi_options")
            try:
                requirement = Requirement(line)
            except InvalidRequirement as e:
                raise ValueError(f"invalid requirement {line!r}: {e}") from None
            if requirement.marker is not None:
                raise ValueError(f"requirement {line!r}: environment markers are not supported")
            if any(canonicalize_name(name) == canonicalize_name(requirement.name) for name in result):
                raise ValueError(f"{requirement.name} is listed twice in requirements")
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
    workspace_name: str | None = None,
    dependencies: dict[str, Any] | Sequence[str] | None = None,
    pypi_dependencies: dict[str, Any] | None = None,
    pypi_options: dict[str, Any] | None = None,
    environments: dict[str, Any] | None = None,
    feature: dict[str, Any] | None = None,
) -> str:
    """Build pixi.toml content from inline config (same options as pixi.toml format)."""
    workspace: dict[str, Any] = {"channels": list(channels), "platforms": list(platforms)}
    if workspace_name:
        workspace["name"] = workspace_name
    manifest: dict[str, Any] = {"workspace": workspace}
    if dependencies:
        manifest["dependencies"] = conda_dependencies(dependencies)
    if pypi_dependencies:
        manifest["pypi-dependencies"] = dict(pypi_dependencies)
    if pypi_options:
        manifest["pypi-options"] = dict(pypi_options)
    features: dict[str, Any] = {}
    for feat_name, feat_cfg in (feature or {}).items():
        if not isinstance(feat_cfg, dict):
            continue
        table = {k: list(feat_cfg[k]) for k in ("channels", "platforms") if k in feat_cfg}
        if "dependencies" in feat_cfg:
            table["dependencies"] = conda_dependencies(feat_cfg["dependencies"])
        if "pypi_dependencies" in feat_cfg:
            table["pypi-dependencies"] = dict(feat_cfg["pypi_dependencies"])
        features[feat_name] = table
    if features:
        manifest["feature"] = features
    if environments:
        manifest["environments"] = dict(environments)
    return tomlkit.dumps(manifest)
