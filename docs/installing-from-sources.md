# Installing from sources

## Released packages

Releases are published to [PyPI](https://pypi.org/project/apache-airflow-providers-pixi/), and
the wheel and sdist of each release are attached to its
[GitHub release](https://github.com/danplischke/apache-airflow-providers-pixi/releases).

```bash
pip install apache-airflow-providers-pixi
```

## From the repository

Install the current `main` branch, or any tag or commit, directly from GitHub:

```bash
pip install "apache-airflow-providers-pixi @ git+https://github.com/danplischke/apache-airflow-providers-pixi@main"
```

## Building the package

The package is built with [flit](https://flit.pypa.io). With [uv](https://docs.astral.sh/uv/):

```bash
git clone https://github.com/danplischke/apache-airflow-providers-pixi
cd apache-airflow-providers-pixi
uv build            # writes the sdist and wheel to dist/
pip install dist/apache_airflow_providers_pixi-*.whl
```

## Trying it in a local Airflow

The repository has a local Airflow for trying the operators, with [just](https://just.systems) and
pixi installed:

```bash
uv sync --group dev
just standalone   # Airflow on SQLite at http://localhost:8080, no login
```

It loads the DAGs in `dev/dags`: `pixi_showcase` runs every operator, sensor and decorator that runs
on the worker (all but the Kubernetes ones) against the sample project in `dev/project`, and `pixi_kubernetes_showcase` runs
`@task.pixi_kubernetes` on the cluster in `~/.kube/config`. Its state lives in `.airflow/`;
`just airflow-reset` deletes it, and `just airflow <command>` runs the Airflow CLI against it, for
example `just airflow dags test pixi_showcase`.

## Building this documentation

```bash
uv sync --group docs
uv run zensical serve   # preview on http://localhost:8000
uv run zensical build   # static site in site/
```

## Testing against other Airflow versions

The `test` job in CI installs from `uv.lock`, which has the newest Airflow and providers. Two more jobs check
the versions users actually install:

- `compat` installs the latest patch of each supported Airflow minor with Airflow's
  [constraints file](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html#constraints-files),
  installs this provider with `--no-deps`, runs `uv pip check` and the tests. Without pixi the integration
  and system tests skip, but they are still imported, so an import that this Airflow version lacks fails
  the leg. To run one leg locally, in a copy of the repository so the project `.venv` is left alone:

    ```bash
    AIRFLOW_VERSION=3.1.8
    uv venv --python 3.12
    uv pip install "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-standard \
      apache-airflow-providers-cncf-kubernetes apache-airflow-providers-common-compat \
      pytest pyyaml jsonschema tomlkit "packaging>=22" \
      -c "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"
    uv pip install --no-deps -e .
    uv pip check
    .venv/bin/python -m pytest tests
    ```

- `lowest-direct` runs `uv sync --resolution lowest-direct --group dev` on Python 3.10, which installs
  every direct dependency at the floor in `pyproject.toml`, and runs the unit tests. It rewrites
  `uv.lock`, so run it in a copy too.

When a new Airflow minor is released, add its latest patch to the `compat` matrix. When the floor
moves, set the provider floors to what that Airflow version's constraints file pins, and keep its
minor as the oldest entry in the matrix (`tests/unit/pixi/test_versions.py` checks this).

## Releasing

Releases are made by pushing a tag. `.github/workflows/release.yml` then runs every QA job on the
tag, checks that the tag is `v` plus the version in `pyproject.toml`, builds the package, uploads it
to PyPI and only then creates the GitHub release with the same files. If any step fails, nothing
after it runs.

1. Set the new version in `pyproject.toml`, `__version__` in `src/airflow/providers/pixi/__init__.py`,
   as the first entry of `versions` in `provider.yaml`, and as the first `## ` heading in
   `docs/changelog.md`. `tests/unit/pixi/test_versions.py` fails until all four agree.
2. Merge to `main` and wait for QA to pass.
3. Tag the merge commit and push the tag:

    ```bash
    git tag v0.2.0
    git push origin v0.2.0
    ```

A version can be uploaded to PyPI only once. If the PyPI step fails because the version already
exists, bump the version and tag again; do not move the tag.
