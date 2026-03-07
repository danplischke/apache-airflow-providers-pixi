# Utility commands for apache-airflow-providers-pixi

default:
    just --list

# Install package and dev dependencies
install:
    pip install -e ".[dev]"

# Lint with ruff
lint:
    ruff check airflow_providers_pixi tests

# Format with ruff
format:
    ruff format airflow_providers_pixi tests

# Fix auto-fixable lint issues
fix:
    ruff check airflow_providers_pixi tests --fix
    ruff format airflow_providers_pixi tests

# Run tests
test:
    pytest tests/ -v

# Run tests with coverage
test-cov:
    pytest tests/ -v --cov=airflow_providers_pixi --cov-report=term-missing

# Type check with pyright
pyright:
    pyright airflow_providers_pixi tests

# Type check with mypy
mypy:
    mypy airflow_providers_pixi

# Run all type checks
typecheck: pyright mypy

# Run all checks (lint, format check, test, typecheck)
check: lint test typecheck

# Ensure formatting is applied
check-format:
    ruff format --check airflow_providers_pixi tests

# Build wheel
build:
    pip wheel . -w dist/

# Install pre-commit hooks
pre-commit-install:
    pre-commit install

# Run pre-commit on all files
pre-commit:
    pre-commit run --all-files

# Update pre-commit hook versions
pre-commit-update:
    pre-commit autoupdate

# Clean build artifacts
clean:
    rm -rf dist/ build/ *.egg-info .pytest_cache .ruff_cache
    find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
