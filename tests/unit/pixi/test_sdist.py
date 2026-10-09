"""The sdist ships ``tests/``, so it must also ship the repository files those tests read."""

from __future__ import annotations

from pathlib import Path, PurePosixPath

import pytest
import tomlkit

ROOT = Path(__file__).parents[3]
SDIST = tomlkit.parse((ROOT / "pyproject.toml").read_text()).unwrap()["tool"]["hatch"]["build"]["targets"]["sdist"]
READ_BY_TESTS = (
    "pyproject.toml",
    "provider.yaml",
    ".github/workflows/qa.yml",
    "docs/changelog.md",
    "dev/dags/pixi_showcase.py",
    "dev/dags/pixi_kubernetes_showcase.py",
    "tests/system/pixi/example_pixi.py",
)


def _in_sdist(path: str) -> bool:
    if path == "pyproject.toml":
        return True
    return any(PurePosixPath(path).is_relative_to(entry) for entry in SDIST["only-include"])


def test_sdist_ships_the_tests() -> None:
    assert "tests" in SDIST["only-include"]


@pytest.mark.parametrize("path", READ_BY_TESTS)
def test_sdist_ships_the_files_the_tests_read(path: str) -> None:
    assert (ROOT / path).exists()
    assert _in_sdist(path)
