# NMDC Migration CLI

Command-Line Interface (CLI) application that you can use to migrate the NMDC database
between two versions of the NMDC schema.

This app was designed to be run in an environment having the following:

1. The programs: `mongodump`, `mongorestore`, and `pip`
2. Permission to create[^1] and edit[^1] roles on the "origin" MongoDB server
3. Permission to read from, and write to[^1], the "origin" MongoDB database
4. Permission to read from, and write to, a distinct "transformer" MongoDB database

[^1]: Not required in `--skip-origin-writes` mode

In practice, we deploy it as follows:

1. In `nmdc-runtime` (here), we have this CLI app. Although it resides in the same repo as the
   Runtime, they are two independent applications and have two independent `pyproject.toml` files.
   This CLI app could be moved to a separate repository without the Runtime realizing it.
2. In `nmdc-runtime` (here), we also have a `Dockerfile` (in the same directory as the CLI app) that
   describes a container image that (a) launches MongoDB in daemon mode and (b) launches the CLI app.
   Coming soon: A container image built from that `Dockerfile` will be hosted publicly on GHCR.
3. In `nmdc-cloud-deployment` (private), we have a normally-suspended CronJob configured to use that
   container image. The CronJob is reconfigured before use, so that the relevant migrator is run and
   the appropriate MongoDB server is used as the "origin" MongoDB server.

## Quick start

Run a container based on a container image hosted on
[GitHub Container Registry](https://github.com/microbiomedata/nmdc-runtime/pkgs/container/nmdc-migration-cli) (GHCR).
For example:

```sh
# Read the latest help messages.
docker run --rm -it ghcr.io/microbiomedata/nmdc-migration-cli:prod --help
docker run --rm -it ghcr.io/microbiomedata/nmdc-migration-cli:prod migrate --help

# Supply any sensitive values to the container via environment variables,
# in order to keep them out of your shell's command history.
export ORIGIN_MONGO_PASSWORD=`cat ~/.my_mongo_password`

# Perform a dry run of a migration.
docker run --rm -it -e ORIGIN_MONGO_PASSWORD ghcr.io/microbiomedata/nmdc-migration-cli:prod \
  --migrator-git-ref v11.26.0 \
  --migrator-module-name migrator_from_11_25_0_to_11_26_0 \
  --collection biosample_set \
  --origin-mongo-host host.docker.internal \
  --origin-mongo-username my_mongo_username \
  --log-file /tmp/nmdc-migration-cli.log \
  --show-diff \
  --skip-origin-writes
```

> You can replace the `:prod` image tag with `:main` above in order to use the container image built
> from the **latest commit** on the `main` branch (as opposed to the one built from the
> **latest GitHub Release**). All of the available container images are listed on
> [GHCR](https://github.com/microbiomedata/nmdc-runtime/pkgs/container/nmdc-migration-cli).

## Development

All commands shown below were designed to be run from the "root" directory of the NMDC Migration CLI project:

```sh
cd db/migrations/cli/
```

### Install dependencies

```sh
uv sync --all-groups
```

### Run doctests

```sh
uv run python -m doctest $(find ./src -name '*.py')
```

> We may simplify this to `pytest --doctest-modules` eventually (`pytest` is not currently a dependency of this app).

### Format and lint code

```sh
uv run ruff format . && uv run ruff check .
```

### Run app

```sh
uv run nmdc-migration-cli --help
```

> The `pyproject.toml` file contains a `[project.scripts]` entry that maps the `nmdc-migration-cli` command to the `app` variable defined within the `src/nmdc_migration_cli/main.py` file.

## Containerization

### Build container

```sh
docker build --progress=plain --tag nmdc-migration-cli:latest .
```

> `--progress=plain` makes it so a given step's output doesn't go away when the step is done. This can help with debugging the build.

### Run container (once built)

```sh
docker run --rm -it nmdc-migration-cli:latest --help
```

> The `--help` parameter will be appended to the `ENTRYPOINT` defined in the `Dockerfile`, which is `uv run --no-sync nmdc-migration-cli`.
