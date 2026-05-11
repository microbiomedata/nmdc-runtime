# NMDC Migration CLI

Command-Line Interface (CLI) application that you can use to migrate the NMDC database
between two versions of the NMDC schema.

This app was designed to be run in an environment having the following:

1. The programs: `mongodump`, `mongorestore`, and `pip`
2. Permission to create[^1] and edit[^1] roles on the "origin" MongoDB server
3. Permission to read from, and write to[^1], the "origin" MongoDB database*
4. Permission to read from, and write to, a distinct "transformer" MongoDB database

[^1]: Not required in `--dry-run` mode

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
uv run python -m doctest src/**/*.py
```

### Format and lint code

```sh
ruff format src/**/*.py && ruff check src/**/*.py
```

### Run app

```sh
uv run nmdc-migration-cli --help
```

> The `pyproject.toml` file contains a `[project.scripts]` entry that maps the `nmdc-migration-cli` command to the `app` variable defined within the `src/main.py` file.

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

> The `--help` parameter will be appended to the `ENTRYPOINT` defined in the `Dockerfile`, which is `uv run nmdc-migration-cli`.
