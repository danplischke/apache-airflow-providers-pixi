set dotenv-load
set shell := ['sh', '-cu']

src := "src tests dev"
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
    uv run mypy

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

# Kubernetes system tests in the current kubectl context, e.g. after `kind create cluster`, as kubernetes.yml does
[group('test')]
test-k8s *args:
    #!/usr/bin/env sh
    set -eu
    kubectl cluster-info > /dev/null 2>&1 || { echo "no Kubernetes cluster in the current kubectl context; try: kind create cluster"; exit 1; }
    export AIRFLOW_HOME="{{ airflow_e2e_home }}" AIRFLOW__CORE__LOAD_EXAMPLES=False
    export AIRFLOW__CORE__DAGS_FOLDER="$PWD/tests/system/pixi"
    uv run airflow db migrate > /dev/null
    PIXI_K8S_E2E_TEST=1 uv run pytest tests/system/pixi/test_example_kubernetes.py {{ args }}

# Tests against Airflow releases with their official constraints, as the compat job does, e.g. `just test-compat 3.1.8`; default: the versions in qa.yml
[group('test')]
test-compat *versions:
    #!/usr/bin/env sh
    set -eu
    versions="{{ versions }}"
    [ -n "$versions" ] || versions=$(sed -n 's/^ *airflow-version: \[\(.*\)\]/\1/p' .github/workflows/qa.yml | tr -d '",')
    for v in $versions; do
        venv="$PWD/.cache/compat/$v"
        echo "== Airflow $v in $venv"
        uv venv --quiet --allow-existing --python 3.12 "$venv"
        export VIRTUAL_ENV="$venv"
        uv pip install --quiet "apache-airflow==$v" apache-airflow-providers-standard \
            apache-airflow-providers-cncf-kubernetes apache-airflow-providers-docker \
            apache-airflow-providers-common-compat \
            pytest pyyaml jsonschema tomlkit "packaging>=22" \
            -c "https://raw.githubusercontent.com/apache/airflow/constraints-$v/constraints-3.12.txt"
        uv pip install --quiet --no-deps -e .
        uv pip check
        "$venv/bin/python" -m pytest tests -q
    done

# Unit tests with every direct dependency at its lowest allowed version on Python 3.10, as the lowest-direct job does
[group('test')]
test-lowest *args:
    #!/usr/bin/env sh
    set -eu
    copy="$PWD/.cache/lowest"
    mkdir -p "$copy"
    rsync -a --delete --exclude .git --exclude .venv --exclude .cache --exclude .pixi --exclude .airflow --exclude site ./ "$copy/"
    cd "$copy"
    uv sync --quiet --resolution lowest-direct --group dev --python 3.10
    uv run --no-sync pytest tests/unit -q {{ args }}

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

# Import the package without the dev dependencies or the cncf.kubernetes extra; a broken provider info breaks `import airflow`
[group('package')]
smoke:
    uv run --isolated --no-default-groups python dev/smoke.py

[group('package')]
lock:
    uv lock

# Upgrade the locked dependencies to their latest allowed versions
[group('package')]
upgrade:
    uv lock --upgrade

# The version hatch-vcs computes for the working tree: the tag on a tagged commit, a .devN version after it
[group('package')]
version:
    @uvx --quiet --from hatchling --with hatch-vcs hatchling version

[group('package')]
versions:
    @git tag -l "v*" --sort=-v:refname | head -10

# Add <ver> to the versions in provider.yaml and check that it is the newest heading in docs/changelog.md, e.g. `just bump 0.2.0`
[group('package')]
bump ver:
    #!/usr/bin/env python3
    import re
    from pathlib import Path

    ver = "{{ ver }}"
    pattern = r"(\d+)\.(\d+)\.(\d+)(?:(a|b|rc)(\d+))?"
    if not re.fullmatch(pattern, ver):
        raise SystemExit(f"not a version: {ver}")

    def key(v):
        major, minor, patch, pre, num = re.fullmatch(pattern, v).groups()
        return (int(major), int(minor), int(patch), {"a": 0, "b": 1, "rc": 2, None: 3}[pre], int(num or 0))

    provider = Path("provider.yaml")
    text = provider.read_text()
    versions = re.search(r"^versions:\n((?:  - .*\n)*)", text, flags=re.M)
    if versions is None:
        raise SystemExit("provider.yaml has no versions list")
    listed = re.findall(r"^  - (.*)$", versions.group(1), flags=re.M)
    if ver in listed and listed[0] != ver:
        raise SystemExit(f"provider.yaml lists {ver} after {listed[0]}")
    if listed and ver not in listed and key(ver) < key(listed[0]):
        raise SystemExit(f"{ver} is lower than {listed[0]}, the newest version in provider.yaml")

    # check before writing, so a failed bump leaves provider.yaml untouched
    headings = re.findall(r"^## (.+?)\s*$", Path("docs/changelog.md").read_text(), flags=re.M)
    if not headings or headings[0] != ver:
        raise SystemExit(f"add '## {ver}' as the first section of docs/changelog.md, then run `just bump {ver}` again")
    if ver not in listed:
        provider.write_text(text[: versions.start(1)] + f"  - {ver}\n" + text[versions.start(1) :])
    print(f"provider.yaml and docs/changelog.md are at {ver}; merge to main, then run `just release {ver}`")

# Tag and push v<ver> from a clean main; the release workflow builds and publishes it
[group('publish')]
release ver:
    #!/usr/bin/env sh
    set -eu
    provider=$(awk '/^versions:/ { getline; sub(/^ *- */, ""); print; exit }' provider.yaml)
    changelog=$(sed -n 's/^## //p' docs/changelog.md | head -1)
    [ "$provider" = "{{ ver }}" ] || { echo "provider.yaml is at $provider, not {{ ver }}; run just bump {{ ver }}"; exit 1; }
    [ "$changelog" = "{{ ver }}" ] || { echo "the newest section of docs/changelog.md is $changelog, not {{ ver }}"; exit 1; }
    [ "$(git branch --show-current)" = "main" ] || { echo "release from main"; exit 1; }
    [ -z "$(git status --porcelain)" ] || { echo "the working tree is not clean"; exit 1; }
    git tag -a "v{{ ver }}" -m "Release v{{ ver }}"
    git push origin "v{{ ver }}"
    echo "Pushed v{{ ver }}; follow the release at $(gh repo view --json url --jq .url)/actions"

# Local Airflow on SQLite at http://localhost:8080 (AIRFLOW_PORT), no login, DAGs in dev/dags; Ctrl+C stops it
[group('airflow')]
standalone:
    #!/usr/bin/env sh
    set -eu
    command -v pixi > /dev/null || echo "pixi is not on PATH, so Pixi tasks will fail: brew install pixi, or see https://pixi.sh/latest/installation/"
    . dev/airflow-env.sh
    exec uv run airflow standalone

# Run the Airflow CLI against the local Airflow, e.g. `just airflow dags test pixi_showcase`
[group('airflow')]
airflow *args:
    #!/usr/bin/env sh
    set -eu
    . dev/airflow-env.sh
    exec uv run airflow {{ args }}

# Delete the local Airflow's database, logs and cached inline environments
[group('airflow')]
airflow-reset:
    rm -rf .airflow

[group('maintenance')]
clean:
    rm -rf dist site .cache .pytest_cache .ruff_cache .coverage coverage.xml
