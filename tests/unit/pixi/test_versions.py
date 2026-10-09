"""Version numbers that must agree before a release; release.yml checks the tag against pyproject.toml."""

from __future__ import annotations

import re
from pathlib import Path

import tomlkit
import yaml
from packaging.requirements import Requirement
from packaging.version import Version

import airflow.providers.pixi

ROOT = Path(__file__).parents[3]
PYPROJECT = tomlkit.parse((ROOT / "pyproject.toml").read_text()).unwrap()
QA_WORKFLOW = yaml.safe_load((ROOT / ".github" / "workflows" / "qa.yml").read_text())


def test_package_version_matches_dunder_version() -> None:
    assert PYPROJECT["project"]["version"] == airflow.providers.pixi.__version__


def test_package_version_is_the_newest_in_provider_yaml() -> None:
    versions = yaml.safe_load((ROOT / "provider.yaml").read_text())["versions"]
    assert str(versions[0]) == PYPROJECT["project"]["version"]


def test_package_version_is_the_newest_in_the_changelog() -> None:
    headings = re.findall(r"^## (.+?)\s*$", (ROOT / "docs" / "changelog.md").read_text(), flags=re.MULTILINE)
    assert headings, "docs/changelog.md has no '## <version>' heading"
    assert headings[0] == PYPROJECT["project"]["version"]


def test_compat_job_covers_the_supported_airflow_versions() -> None:
    airflow = next(
        Requirement(r) for r in PYPROJECT["project"]["dependencies"] if Requirement(r).name == "apache-airflow"
    )
    tested = [Version(v) for v in QA_WORKFLOW["jobs"]["compat"]["strategy"]["matrix"]["airflow-version"]]
    assert all(v in airflow.specifier for v in tested)
    floor = next(Version(s.version) for s in airflow.specifier if s.operator == ">=")
    assert min(tested).release[:2] == floor.release[:2]
