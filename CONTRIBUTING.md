# Contributing

Thanks for helping with the Pixi provider for Apache Airflow. Bug reports, fixes, docs and new features
are all welcome. For a larger change, open an issue first so we can agree on the approach before you
write the code.

## Reporting a security issue

Don't open a public issue. Use
[private vulnerability reporting](https://github.com/danplischke/apache-airflow-providers-pixi/security/advisories/new)
instead. [docs/security.md](docs/security.md) describes the provider's security model.

## Setting up

You need [uv](https://docs.astral.sh/uv/), [just](https://just.systems) and
[pixi](https://pixi.sh/latest/installation/) 0.81.0 or newer. Then:

```bash
just dev
```

This installs the package with the `dev` and `docs` groups into `.venv` and installs the pre-commit
hooks. Run `just` to list all recipes.

To try your change in a running Airflow, `just standalone` starts one at http://localhost:8080 with the
DAGs in [dev/dags](dev/dags). They run in the Pixi project in [dev/project](dev/project).

## Making a change

1. Fork the repository and create a branch from `main`.
2. Make the change, with tests and docs (see below).
3. Run `just chore` to format and lint, then `just check` to run what CI runs.
4. Open a pull request against `main`.

`main` is protected:

- Every change goes through a pull request and needs an approval from a code owner.
- Commits must be [signed](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits).
- All CI checks must pass.

## Code style

- [ruff](https://docs.astral.sh/ruff/) formats and lints the code, with a line length of 120. The
  pre-commit hooks and `just lint` use the ruff version from `uv.lock`, the same one CI uses.
- `src/airflow/providers/pixi/runtime` runs inside the Pixi environment, not on the worker. It must
  pass `mypy --strict` and can only import the standard library.
- Python 3.10 is the oldest supported version. `just test-lowest` runs the unit tests with every
  direct dependency at its lowest allowed version.

## Tests

| Recipe | What it runs | Needs |
| --- | --- | --- |
| `just test` | Unit tests in `tests/unit`, with a fake pixi | nothing else |
| `just test-integration` | `tests/integration` against the real pixi | pixi, access to conda-forge |
| `just test-system` | DAGs in `tests/system/pixi` through `dag.test()` | pixi, conda-forge |
| `just test-k8s` | Kubernetes system tests | a cluster in the current kubectl context, e.g. `kind create cluster` |
| `just test-compat` | All tests against the Airflow releases in `qa.yml`, with their constraints | network |

Extra arguments are passed to pytest, e.g. `just test -k serializer`.

Every behaviour change needs a unit test. If the change depends on how pixi or Airflow really behave,
add an integration or system test too. [System Tests](docs/system-tests.md) has more detail.

## Documentation

The docs are in [docs](docs) and are built with [Zensical](https://zensical.org). `just docs` serves
them locally, and `just docs-build` builds them with the same strict settings as CI. Update the guide
for any operator, decorator or argument you change, and add new pages to `nav` in
[zensical.toml](zensical.toml).

For any change users will notice, add an entry to [docs/changelog.md](docs/changelog.md) under the
upcoming version. If the top section is a version that's already released, add a new section for the
next version.

## Provider metadata

[provider.yaml](provider.yaml) and
[get_provider_info.py](src/airflow/providers/pixi/get_provider_info.py) must describe the same
operators, decorators and connection types. The unit tests check that they match, that they're valid
against Airflow's schema, and that every module they list can be imported. A broken provider info
breaks `import airflow` for everyone who has the package installed, which is what `just smoke` checks.

## Releases

Maintainers release from `main`:

1. `just bump <version>` adds the version to `provider.yaml` and checks that it's the newest heading
   in the changelog. Merge that change.
2. `just release <version>` tags `v<version>` and pushes the tag. The release workflow runs QA, builds
   the package, publishes it to PyPI and creates the GitHub release.

Only repository admins can push `v*` tags.

## License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
