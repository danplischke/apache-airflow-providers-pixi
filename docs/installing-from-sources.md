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

It loads the DAGs in `dev/dags`: `pixi_showcase` runs every operator and decorator on the worker
against the sample project in `dev/project`, and `pixi_kubernetes_showcase` runs
`@task.pixi_kubernetes` on the cluster in `~/.kube/config`. Its state lives in `.airflow/`;
`just airflow-reset` deletes it, and `just airflow <command>` runs the Airflow CLI against it, for
example `just airflow dags test pixi_showcase`.

## Building this documentation

```bash
uv sync --group docs
uv run zensical serve   # preview on http://localhost:8000
uv run zensical build   # static site in site/
```
