set dotenv-load
set shell := ['sh', '-cu']

src := "src tests"
airflow_e2e_home := env_var_or_default("AIRFLOW_E2E_HOME", "/tmp/airflow-e2e")

@_:
    just --list

# Install the package with the dev and docs groups, and the pre-commit hooks
[group('development')]
dev:
    uv sync --group dev --group docs
    uv run pre-commit install

[group('development')]
format:
    uv run ruff format {{ src }}
    uv run ruff check {{ src }} --fix

# Lint and format check, as the CI does
[group('development')]
lint:
    uv run ruff check {{ src }}
    uv run ruff format {{ src }} --check

[group('development')]
chore: format lint

# Run the pre-commit hooks on all files
[group('development')]
pc:
    uv run pre-commit run --all-files

# Unit tests; a fake pixi runs the command with this Python, e.g. `just test -k serializer`
[group('test')]
test *args:
    uv run pytest tests/unit {{ args }}

[group('test')]
test-cov:
    uv run pytest tests/unit --cov=src/airflow/providers/pixi --cov-report=term-missing

# Integration tests against the real pixi on PATH; needs access to conda-forge
[group('test')]
test-integration *args:
    PIXI_INTEGRATION_TEST=1 uv run pytest tests/integration {{ args }}

# System tests through dag.test(), with Airflow's task runner and real pixi
[group('test')]
test-system *args:
    #!/usr/bin/env sh
    set -eu
    export AIRFLOW_HOME="{{ airflow_e2e_home }}" AIRFLOW__CORE__LOAD_EXAMPLES=False
    export AIRFLOW__CORE__DAGS_FOLDER="$PWD/tests/system/pixi"
    uv run airflow db migrate > /dev/null
    PIXI_E2E_TEST=1 uv run pytest tests/system/pixi {{ args }}

[group('test')]
test-all: test test-integration test-system

# Lint, unit tests, docs and package build, as the CI does
[group('test')]
check: lint test docs-build build smoke

[group('docs')]
docs:
    uv run --group docs zensical serve

[group('docs')]
docs-build:
    uv run --group docs zensical build --strict --clean

# Build the sdist and wheel into dist/ and check their metadata
[group('package')]
build:
    rm -rf dist
    uv build
    uvx 'twine>=7.0.0' check dist/*

# Import the package without the dev dependencies; a broken provider info breaks `import airflow`
[group('package')]
smoke:
    uv run --isolated --no-default-groups python -c "import airflow.providers.pixi; from airflow.providers.pixi.get_provider_info import get_provider_info; get_provider_info(); from airflow.sdk import task; assert hasattr(task, 'pixi'); print('smoke test OK')"

[group('package')]
lock:
    uv lock

# Upgrade the locked dependencies to their latest allowed versions
[group('package')]
upgrade:
    uv lock --upgrade

[group('package')]
version:
    @uv version --short

[group('package')]
versions:
    @git tag -l "v*" --sort=-v:refname | head -10

# Set the version in pyproject.toml, __init__.py and provider.yaml, e.g. `just bump 0.2.0`
[group('package')]
bump ver:
    #!/usr/bin/env python3
    import re
    from pathlib import Path

    ver = "{{ ver }}"
    if not re.fullmatch(r"\d+\.\d+\.\d+((a|b|rc)\d+)?", ver):
        raise SystemExit(f"not a version: {ver}")

    def sub(path, pattern, replacement):
        file = Path(path)
        text, count = re.subn(pattern, replacement, file.read_text(), count=1, flags=re.M)
        if count != 1:
            raise SystemExit(f"{path}: no match for {pattern}")
        file.write_text(text)

    sub("pyproject.toml", r'^version = ".*"$', f'version = "{ver}"')
    sub("src/airflow/providers/pixi/__init__.py", r'^__version__ = ".*"$', f'__version__ = "{ver}"')
    # provider.yaml lists every release, newest first
    if f"  - {ver}\n" not in Path("provider.yaml").read_text():
        sub("provider.yaml", r"^versions:\n", f"versions:\n  - {ver}\n")
    if f"## {ver}" not in Path("docs/changelog.md").read_text():
        print(f"docs/changelog.md has no '## {ver}' section yet")
    print(f"version {ver}; run `just lock` to update uv.lock")

# Tag and push v<ver> from a clean main; the release workflow builds and publishes it
[group('publish')]
release ver:
    #!/usr/bin/env sh
    set -eu
    [ "$(uv version --short)" = "{{ ver }}" ] || { echo "pyproject.toml is at $(uv version --short), not {{ ver }}; run just bump {{ ver }}"; exit 1; }
    [ "$(git branch --show-current)" = "main" ] || { echo "release from main"; exit 1; }
    [ -z "$(git status --porcelain)" ] || { echo "the working tree is not clean"; exit 1; }
    git tag -a "v{{ ver }}" -m "Release v{{ ver }}"
    git push origin "v{{ ver }}"
    echo "Pushed v{{ ver }}; follow the release at $(gh repo view --json url --jq .url)/actions"

[group('maintenance')]
clean:
    rm -rf dist site .cache .pytest_cache .ruff_cache .coverage coverage.xml
