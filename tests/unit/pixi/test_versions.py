"""Version numbers that must agree before a release; release.yml checks the tag against provider.yaml and the changelog.

The package version itself comes from the git tag through hatch-vcs.
"""

from __future__ import annotations

import importlib.metadata
import re
from pathlib import Path

import pytest
import tomlkit
import yaml
from packaging.requirements import Requirement
from packaging.version import Version

import airflow.providers.pixi

ROOT = Path(__file__).parents[3]
PYPROJECT = tomlkit.parse((ROOT / "pyproject.toml").read_text()).unwrap()
QA_WORKFLOW = yaml.safe_load((ROOT / ".github" / "workflows" / "qa.yml").read_text())


def test_dunder_version_is_the_installed_version() -> None:
    assert airflow.providers.pixi.__version__ == importlib.metadata.version("apache-airflow-providers-pixi")


def test_dunder_version_without_an_installed_distribution(monkeypatch: pytest.MonkeyPatch) -> None:
    def not_installed(name: str) -> str:
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, "version", not_installed)
    assert airflow.providers.pixi.__version__ == PYPROJECT["tool"]["hatch"]["version"]["fallback-version"]


def test_newest_provider_yaml_version_is_the_newest_in_the_changelog() -> None:
    versions = yaml.safe_load((ROOT / "provider.yaml").read_text())["versions"]
    headings = re.findall(r"^## (.+?)\s*$", (ROOT / "docs" / "changelog.md").read_text(), flags=re.MULTILINE)
    assert headings, "docs/changelog.md has no '## <version>' heading"
    assert str(versions[0]) == headings[0]


def test_compat_job_covers_the_supported_airflow_versions() -> None:
    airflow = next(
        Requirement(r) for r in PYPROJECT["project"]["dependencies"] if Requirement(r).name == "apache-airflow"
    )
    tested = [Version(v) for v in QA_WORKFLOW["jobs"]["compat"]["strategy"]["matrix"]["airflow-version"]]
    assert all(v in airflow.specifier for v in tested)
    floor = next(Version(s.version) for s in airflow.specifier if s.operator == ">=")
    assert min(tested).release[:2] == floor.release[:2]
