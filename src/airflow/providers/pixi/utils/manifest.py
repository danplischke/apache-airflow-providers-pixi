"""Inline Pixi manifests: ``pixi.toml`` written from operator arguments."""

from __future__ import annotations

import functools
import importlib.resources
import json
import re
from collections.abc import Sequence
from typing import Any

import tomlkit
from jsonschema.exceptions import ValidationError, best_match
from jsonschema.protocols import Validator
from jsonschema.validators import Draft7Validator, validator_for
from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

from airflow.providers.pixi.utils.pixi import MIN_PIXI_VERSION

SCHEMA_FILE = "pixi_manifest.schema.json"
"""Pixi's JSON Schema of ``pixi.toml`` for :data:`MIN_PIXI_VERSION`, vendored next to this module."""

_REQUIREMENT_COMMENT = re.compile(r"(^|\s)#.*$")
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


def _feature_table(name: str, config: Any) -> Any:
    if not isinstance(config, dict):
        return config
    table = dict(config)
    for key in ("channels", "platforms"):
        if key in table:
            table[key] = list(table[key])
    if "dependencies" in table:
        table["dependencies"] = conda_dependencies(table["dependencies"])
    if "pypi_dependencies" in table:
        if "pypi-dependencies" in table:
            raise ValueError(f"feature {name!r} has both pypi_dependencies and pypi-dependencies")
        table["pypi-dependencies"] = dict(table.pop("pypi_dependencies"))
    return table


def manifest_table(
    *,
    channels: Sequence[str],
    platforms: Sequence[str],
    workspace_name: str | None = None,
    dependencies: dict[str, Any] | Sequence[str] | None = None,
    pypi_dependencies: dict[str, Any] | None = None,
    pypi_options: dict[str, Any] | None = None,
    environments: dict[str, Any] | None = None,
    feature: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Assemble an inline manifest as the dict :func:`build_pixi_toml` writes, with ``pixi.toml``'s key names."""
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
    if feature:
        manifest["feature"] = {name: _feature_table(name, config) for name, config in feature.items()}
    if environments:
        manifest["environments"] = dict(environments)
    return manifest


@functools.cache
def _validator() -> Validator:
    schema = json.loads(importlib.resources.files(__package__).joinpath(SCHEMA_FILE).read_bytes())
    return validator_for(schema, default=Draft7Validator)(schema)


def _toml_path(path: Sequence[str | int]) -> str:
    """``feature.gpu.dependencies."ruamel.yaml"``: where an error is, as a TOML key."""
    parts: list[str] = []
    for key in path:
        if isinstance(key, int):
            parts[-1] += f"[{key}]"
        else:
            parts.append(key if re.fullmatch(r"[A-Za-z0-9_-]+", key) else json.dumps(key))
    return ".".join(parts)


def _schema_types(error: ValidationError) -> list[str]:
    types = error.validator_value
    return [types] if isinstance(types, str) else list(types)


def _wrong_type(error: ValidationError) -> bool:
    """Whether ``error`` says the value itself, not something inside it, has another type than the schema's."""
    return error.validator == "type" and not error.relative_path


def _relevant_error(error: ValidationError) -> ValidationError:
    """Return the error in an ``anyOf`` / ``oneOf`` branch that explains ``error`` best, or ``error`` itself.

    A branch for another type than the value's (a string where a table was given) is not the one meant; of the
    others, the one with the fewest errors, and then the fewest unexpected keys, is the closest to the value.
    """
    while error.validator in ("anyOf", "oneOf") and error.context:
        branches: dict[Any, list[ValidationError]] = {}
        for sub in error.context:
            branches.setdefault(sub.relative_schema_path[0], []).append(sub)
        candidates = [errors for errors in branches.values() if not (len(errors) == 1 and _wrong_type(errors[0]))]
        if not candidates:
            return error

        def closeness(errors: list[ValidationError]) -> tuple[int, int]:
            extra = sum(len(_unexpected(e)) for e in errors if e.validator == "additionalProperties")
            return len(errors), extra

        error = best_match(min(candidates, key=closeness))
    return error


def _unexpected(error: ValidationError) -> list[str]:
    known = error.schema.get("properties", {})
    patterns = error.schema.get("patternProperties", {})
    return [k for k in error.instance if k not in known and not any(re.search(p, k) for p in patterns)]


_JSON_TYPES = {bool: "boolean", int: "integer", float: "number", str: "string", list: "array", dict: "object"}


def _message(error: ValidationError) -> str:
    """Return what is wrong, without the value itself: a manifest may hold an index URL with a password."""
    kind = _JSON_TYPES.get(type(error.instance), type(error.instance).__name__)
    value = error.validator_value
    if "propertyNames" in error.relative_schema_path:
        return f"the key {error.instance!r} is not allowed"
    if error.validator == "type":
        return f"must be of type {' or '.join(map(repr, _schema_types(error)))}, not {kind!r}"
    if error.validator in ("anyOf", "oneOf"):
        types = [t for sub in error.context if _wrong_type(sub) for t in _schema_types(sub)]
        if types:
            return f"must be of type {' or '.join(map(repr, dict.fromkeys(types)))}, not {kind!r}"
        return "matches none of the allowed forms"
    if error.validator == "additionalProperties":
        return error.message
    if error.validator == "enum":
        return f"must be one of {', '.join(map(repr, value))}"
    if error.validator == "const":
        return f"must be {value!r}"
    if error.validator == "pattern":
        return f"must match the pattern {value!r}"
    if error.validator in ("minLength", "minItems", "minProperties") and value == 1:
        return "must not be empty"
    if error.validator in _BOUNDS:
        return _BOUNDS[error.validator].format(value)
    if error.validator == "required":
        return error.message
    if error.validator == "not":
        return "is not allowed here"
    return f"does not satisfy the schema's {error.validator!r}"


_BOUNDS = {
    "minLength": "must be at least {} characters long",
    "maxLength": "must be at most {} characters long",
    "minItems": "must have at least {} items",
    "maxItems": "must have at most {} items",
    "minProperties": "must have at least {} keys",
    "maxProperties": "must have at most {} keys",
    "minimum": "must be at least {}",
    "exclusiveMinimum": "must be greater than {}",
}


def check_manifest(manifest: dict[str, Any]) -> None:
    """Raise ``ValueError`` if ``manifest`` does not follow pixi's manifest schema for :data:`MIN_PIXI_VERSION`.

    The message names the first relevant problem as a TOML key path, such as ``pypi-options``, and what is
    wrong there.
    """
    error = best_match(_validator().iter_errors(manifest))
    if error is None:
        return
    error = _relevant_error(error)
    where = _toml_path(error.absolute_path) or "the top level"
    raise ValueError(
        f"invalid inline pixi manifest at {where}: {_message(error)}. The manifest follows the schema of pixi "
        f"{MIN_PIXI_VERSION}; pass validate_manifest=False for keys only a newer pixi on the workers knows"
    )


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
    validate_manifest: bool = True,
) -> str:
    """Build pixi.toml content from inline config (same options as pixi.toml format).

    :param validate_manifest: check the manifest against pixi's schema first (:func:`check_manifest`).
    """
    manifest = manifest_table(
        channels=channels,
        platforms=platforms,
        workspace_name=workspace_name,
        dependencies=dependencies,
        pypi_dependencies=pypi_dependencies,
        pypi_options=pypi_options,
        environments=environments,
        feature=feature,
    )
    if validate_manifest:
        check_manifest(manifest)
    return tomlkit.dumps(manifest)
