from __future__ import annotations

import json
import stat
import sys
from pathlib import Path
from typing import Any

import pytest

# Stands in for pixi: answers `--version`, or records the call, then runs the command after
# `run --manifest-path M [--environment E]` as a child process, as pixi does, with this interpreter as `python`.
FAKE_PIXI = """#!{python}
import json, os, subprocess, sys

args = sys.argv[1:]
if args == ["--version"]:
    with open({version_calls!r}, "a") as f:
        f.write("--version\\n")
    print("pixi {version}")
    sys.exit(0)
keys = ("PIXI_CACHE_DIR", "UV_CACHE_DIR", "PIP_CACHE_DIR", "PYTHONUNBUFFERED")
with open({calls!r}, "a") as f:
    f.write(json.dumps({{"argv": args, "cwd": os.getcwd(), "env": {{k: os.environ.get(k) for k in keys}}}}) + "\\n")
args = args[1:]
while args[0] in ("--manifest-path", "--environment"):
    args = args[2:]
if args[0] == "python":
    args[0] = sys.executable
sys.exit(subprocess.call(args))
"""


class FakePixi:
    def __init__(self, directory: Path, version: str = "0.81.0") -> None:
        self.path = directory / "pixi"
        self._calls = directory / "calls.jsonl"
        self._version_calls = directory / "version_calls"
        script = FAKE_PIXI.format(
            python=sys.executable, calls=str(self._calls), version_calls=str(self._version_calls), version=version
        )
        self.path.write_text(script)
        self.path.chmod(self.path.stat().st_mode | stat.S_IEXEC)

    @property
    def version_calls(self) -> int:
        return len(self._version_calls.read_text().splitlines()) if self._version_calls.exists() else 0

    @property
    def calls(self) -> list[dict[str, Any]]:
        if not self._calls.exists():
            return []
        return [json.loads(line) for line in self._calls.read_text().splitlines()]


@pytest.fixture
def fake_pixi(tmp_path: Path) -> FakePixi:
    directory = tmp_path / "fake_pixi"
    directory.mkdir()
    return FakePixi(directory)


@pytest.fixture
def make_fake_pixi(tmp_path: Path):
    """Build a fake pixi that reports ``version``."""

    def make(version: str) -> FakePixi:
        directory = tmp_path / f"fake_pixi_{version}"
        directory.mkdir()
        return FakePixi(directory, version=version)

    return make
