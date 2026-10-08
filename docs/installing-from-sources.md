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

## Building this documentation

```bash
uv sync --group docs
uv run zensical serve   # preview on http://localhost:8000
uv run zensical build   # static site in site/
```
