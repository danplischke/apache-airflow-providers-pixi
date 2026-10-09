# Pixi Connection

The Pixi connection type holds the credentials of a private conda channel or PyPI index, such as a
channel on [prefix.dev](https://prefix.dev), on anaconda.org, or in Artifactory or Nexus. Operators
that run pixi on the worker use it through `pixi_conn_id`; the hook is
[`PixiHook`][airflow.providers.pixi.hooks.pixi.PixiHook].

## Configuring the connection

Host
:   The host of the channel or index, such as `repo.prefix.dev`. A URL is reduced to its host name, so
    `https://repo.prefix.dev/my-channel` works too. `*.prefix.dev` matches every subdomain, for conda
    channels only.

Username (login)
:   The username, for `basic_http` and `netrc`. Leave it empty for a token.

Password
:   The token, or the password for `basic_http` and `netrc`. Required. It is masked in the task log.

Auth type
:   How pixi sends the credentials:

    | `auth_type` | Sent as | Typical server |
    |---|---|---|
    | `auto` (default) | `basic_http` if the connection has a username, otherwise `bearer_token` | |
    | `bearer_token` | an `Authorization: Bearer <token>` header | prefix.dev |
    | `conda_token` | the token in the channel URL | anaconda.org, quetz |
    | `basic_http` | HTTP basic authentication | Artifactory, Nexus |
    | `netrc` | a netrc entry, for a PyPI index | any index that takes a username and password |

    On Airflow 3.2 and newer, the connection form has an **Auth type** field for it and no Extra field. On
    Airflow 3.1, the form has no Auth type field: put a JSON object in **Extra** instead, such as
    `{"auth_type": "conda_token"}`. Outside the form, on every version, `auth_type` goes into the extra, as in
    the examples below.

Schema and Port are not used.

## Examples

A prefix.dev token, as an environment variable of the workers:

```bash
export AIRFLOW_CONN_PREFIX_DEV='{"conn_type": "pixi", "host": "repo.prefix.dev", "password": "pfx_..."}'
```

A username and password for Artifactory, with the CLI:

```bash
airflow connections add artifactory \
    --conn-type pixi \
    --conn-host artifactory.example.com \
    --conn-login ci-user \
    --conn-password "$ARTIFACTORY_PASSWORD"
```

An anaconda.org token:

```bash
airflow connections add anaconda \
    --conn-type pixi \
    --conn-host conda.anaconda.org \
    --conn-password "$ANACONDA_TOKEN" \
    --conn-extra '{"auth_type": "conda_token"}'
```

A private PyPI index. The index itself goes, without credentials, into the `[pypi-options]` table
of the project's `pixi.toml`, for example `extra-index-urls = ["https://pypi.example.com/simple"]`,
and the task uses that project with `pixi_project_path` or `pixi_toml_path`:

```bash
airflow connections add private_pypi \
    --conn-type pixi \
    --conn-host pypi.example.com \
    --conn-login ci-user \
    --conn-password "$PYPI_PASSWORD" \
    --conn-extra '{"auth_type": "netrc"}'
```

## Using the connection

Pass one connection id, or a list with one connection per host:

```python
PixiOperator(
    task_id="train",
    pixi_project_path="training-env",
    lock_mode="locked",
    python_callable="train:main",
    pixi_conn_id=["prefix_dev", "private_pypi"],
)
```

`pixi_conn_id` works for [`PixiOperator`](../operators/pixi.md), [`@task.pixi`](../decorators/pixi.md),
[`PixiBashOperator`](../operators/bash.md), `@task.pixi_bash`, [`PixiTaskOperator`](../operators/task.md),
[`PixiSensor`](../sensors/pixi.md) and `@task.pixi_sensor`, and can be set for every task through
`default_args`. Two connections for the same host fail the task.

## How the credentials reach pixi

Pixi reads conda channel credentials from a JSON file and PyPI index credentials from a netrc file
([pixi's authentication docs](https://pixi.sh/latest/deployment/authentication/)). For each run, the
operator reads the connections, writes the credentials it needs to temporary files readable only by
the worker's user, and points pixi at them:

- `RATTLER_AUTH_FILE`, for every connection except `netrc` ones. While it is set, pixi reads no other
  credentials, so the file also holds the entries of the credentials file pixi would read otherwise: the
  one `RATTLER_AUTH_FILE` named before, or `~/.rattler/credentials.json`. A connection replaces an entry
  for the same host. Credentials in the system keychain, such as those stored by `pixi auth login` on a
  desktop, are not copied.
- `NETRC`, for `netrc` connections. The file starts with the connections' entries, followed by the
  netrc file `NETRC` named before, or `~/.netrc`. Keep a host in one of the two, since netrc readers
  differ in which entry they use when a host appears twice.

Both files are removed when the run ends, also when it fails. The values never pass through templates.

## Other credentials

For credentials that are not a channel's or an index's, such as a database password the callable
needs, use `env_from_connections` with a connection of any type; see
[Environment variables](../operators/pixi.md#from-variables-and-connections).
