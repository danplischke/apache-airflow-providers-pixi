from __future__ import annotations

import json
import stat
import sys
from pathlib import Path
from typing import Any

import pytest

# Stands in for pixi: records the call, then runs the command after `run --manifest-path M [--environment E]`
# as a child process, as pixi does, with this interpreter as `python`.
FAKE_PIXI = """#!{python}
import json, os, subprocess, sys

args = sys.argv[1:]
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
    def __init__(self, directory: Path) -> None:
        self.path = directory / "pixi"
        self._calls = directory / "calls.jsonl"
        self.path.write_text(FAKE_PIXI.format(python=sys.executable, calls=str(self._calls)))
        self.path.chmod(self.path.stat().st_mode | stat.S_IEXEC)

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
