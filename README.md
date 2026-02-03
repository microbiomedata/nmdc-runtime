A runtime system for NMDC data management and orchestration.

## Service Status

http://nmdcstatus.polyneme.xyz/

## How It Fits In

* [issues](https://github.com/microbiomedata/issues)  
tracks issues related to NMDC, which may necessitate work across multiple repos.
  
* [nmdc-schema](https://github.com/microbiomedata/nmdc-schema/)
houses the LinkML schema specification, as well as generated artifacts (e.g. JSON Schema).

* [nmdc-server](https://github.com/microbiomedata/nmdc-server)
houses code specific to the data portal -- its database, back-end API, and front-end application.

* Workflows — documented in the [workflows](https://docs.microbiomedata.org/workflows/) section of the NMDC documentation website — take source data and produce computed data.

* This repo (nmdc-runtime)
   * houses code that takes source data and computed data, and transforms it
     to broadly accommodate downstream applications such as the data portal
   * manages execution of the above (i.e., lightweight data transformations) and also
     of computationally- and data-intensive workflows performed at other sites,
     ensuring that claimed jobs have access to needed configuration and data resources.

## Data exports

The NMDC metadata as of 2021-10 is available here:

https://drs.microbiomedata.org/ga4gh/drs/v1/objects/sys086d541

The link returns a [GA4GH DRS API bundle object record](https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/#_drs_datatypes), with the NMDC metadata collections (study_set, biosample_set, etc.) as contents, each a DRS API blob object.

For example the blob for the study_set collection export, named "study_set.jsonl.gz", is listed with DRS API ID "sys0xsry70". Thus, it is retrievable via

https://drs.microbiomedata.org/ga4gh/drs/v1/objects/sys0xsry70

The returned blob object record lists https://nmdc-runtime.files.polyneme.xyz/nmdcdb-mongoexport/2021-10-14/study_set.jsonl.gz as the url for an access method.

The 2021-10 exports are currently all accessible at `https://nmdc-runtime.files.polyneme.xyz/nmdcdb-mongoexport/2021-10-14/${COLLECTION_NAME}.jsonl.gz`, but the DRS API indirection allows these links to change in the future, for mirroring via other URLs, etc. So, the DRS API links should be the links you share.

## Overview

The runtime features:

1. [Dagster](https://docs.dagster.io/concepts) orchestration:
    - dagit - a web UI to monitor and manage the running system.
    - dagster-daemon - a service that triggers pipeline runs based on time or external state.
    - PostgresSQL database - for storing run history, event logs, and scheduler state.
    - workspace code
      - Code to run is loaded into a Dagster `workspace`. This code is loaded from
         one or more dagster `repositories`. Each Dagster `repository` may be run with a different
         Python virtual environment if need be, and may be loaded from a local Python file or
         `pip install`ed from an external source. In our case, each Dagster `repository` is simply
         loaded from a Python file local to the nmdc-runtime GitHub repository, and all code is
         run in the same Python environment.
      - A Dagster repository consists of `solids` and `pipelines`,
         and optionally `schedules` and `sensors`.
         - `solids` represent individual units of computation
         - `pipelines` are built up from solids
         - `schedules` trigger recurring pipeline runs based on time
         - `sensors` trigger pipeline runs based on external state
      - Each `pipeline` can declare dependencies on any runtime `resources` or additional
         configuration. There are MongoDB `resources` defined, as well as `preset`
         configuration definitions for both "dev" and "prod" `modes`. The `preset`s tell Dagster to
         look to a set of known environment variables to load resources configurations, depending on
         the `mode`.
   
2. A MongoDB database supporting write-once, high-throughput internal
data storage by the nmdc-runtime FastAPI instance.
   
3. A [FastAPI](https://fastapi.tiangolo.com/) service to interface with the orchestrator and
database, as a hub for data management and workflow automation.

## Local Development

Ensure Docker (and Docker Compose) are installed; and the Docker engine is running.

```shell
docker --version
docker compose version
docker info
```

Ensure the permissions of `./.docker/mongoKeyFile` are such that only the file's owner can read or write the file.

```shell
chmod 600 ./.docker/mongoKeyFile
```

Ensure you have a `.env` file for the Docker services to source from. You may copy `.env.example` to
`.env` (which is gitignore'd) to get started.

```shell
cp .env.example .env
```

Create environment variables in your shell session, based upon the contents of the `.env` file.

```shell
set -a # automatically export all variables
source .env
set +a
```

If you are connecting to resources that require an SSH tunnel—for example, a MongoDB server that is only accessible on 
the NERSC network—set up the SSH tunnel.

The following command could be useful to you, either directly or as a template (see `Makefile`).

```shell
make nersc-mongo-tunnels
```

**Before you spin up the stack**, consider customizing the base site's site client credentials as described in the section below. Note that the default credentials are sufficient for most developers.

Finally, spin up the Docker Compose stack.

```bash
make up-dev
```

Docker Compose is used to start local MongoDB and PostgresSQL (used by Dagster) instances, as well
as a Dagster web server (dagit) and daemon (dagster-daemon).

The Dagit web server is viewable at http://127.0.0.1:3000/. 

The FastAPI service is viewable at http://127.0.0.1:8000/ -- e.g., rendered documentation at
http://127.0.0.1:8000/redoc/.

### (Optional) Customize the base site's site client credentials

Some environment variables in the `.env` file are only processed during the first boot of the FastAPI container; e.g., the `API_SITE_CLIENT_ID` and `API_SITE_CLIENT_SECRET` environment variables, which dictate the credentials of the base site's site client.

To people who want the base site's site client credentials to have specific values (e.g., to match something in a dependent application), we recommend customizing those environment variables **before** starting up the FastAPI container.

In case you have already started up the FastAPI container (this is common), all is not lost! You can still customize the credentials—here's how:

1. Stop the containers that depend upon Mongo: `$ docker compose stop fastapi dagster-daemon dagster-dagit`
2. Use a Mongo client (e.g. MongoDB Compass) to connect to the Mongo server running in the `mongo` container.
3. Use that Mongo client to delete the base site from the `sites` collection in the `nmdc` database.

   ```js
   // Replace "site-id" with the ID of the base site, which you can get
   // from the `API_SITE_ID` environment variable in your `.env` file.
   db.getCollection("sites").deleteOne({
      id: "site-id",  // e.g. "nmdc-runtime"
   });
   ```

4. Customize the `API_SITE_CLIENT_ID` and `API_SITE_CLIENT_SECRET` environment variables in your `.env` file.
5. Restart the containers you stopped earlier: `$ docker compose start fastapi dagster-daemon dagster-dagit`

When the FastAPI application starts up, it will create a site client having the specified `client_id` and `client_secret`.

### Dependency management

We use [`uv`](https://docs.astral.sh/uv/) to manage dependencies of the application. Here's how you can use `uv` both on your host machine and within a container in the Docker Compose stack.

#### On the host

Although we typically run the application within a container, some developers prefer that application's dependencies be installed locally also (so that their code editors will provide auto-completion, type checking, etc.).

Here's how you can install the application's dependencies locally:

```sh
uv sync
```

That will...
1. (If you made changes to `pyproject.toml`) **Update the lock file** (at `uv.lock`) to reflect those changes
2. (If a Python virtual environment doesn't exist at `.venv/` yet) **Create a Python virtual environment** at `.venv/`
3. (If the Python virtual environment and `uv.lock` files are out of sync) **Synchronize the Python virtual environment** with `uv.lock` (by installing and uninstalling packages)

> Note: Long term, we may implement a [devcontainer](https://containers.dev/) for this project, which will streamline the process of setting up a local development environment.

#### In the Docker Compose stack

In the Docker Compose stack, the Python virtual environment is located at the path specified by the `VIRTUAL_ENV` environment variable (which is defined in the `Dockerfile`) instead of at `.venv/`. That helps with containerization, but it deviates from `uv`'s default behavior, which is to use the Python virtual environment at `.venv/`. So, when running `uv` commands within the Docker Compose stack, we always include the [`--active`](https://docs.astral.sh/uv/reference/cli/#uv-sync--active) flag (which tells `uv` to use the Python virtual environment at the path specified by `VIRTUAL_ENV`).

Here's how you can install the application's dependencies within a container in the Docker Compose stack:

```sh
uv sync --active
```

## Local Testing

Tests can be found in `tests` and are run with the following commands:

```bash
make up-test
make test

# Run a Specific test file eg. tests/test_api/test_endpoints.py
make test ARGS="tests/test_api/test_endpoints.py"

docker compose --file docker-compose.test.yml run test
```

As you create Dagster solids and pipelines, add tests in `tests/` to check that your code behaves as
desired and does not break over time.

[For hints on how to write tests for solids and pipelines in Dagster, see their documentation
tutorial on Testing](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops).

### Performance profiling

We use a tool called [Pyinstrument](https://pyinstrument.readthedocs.io) to profile the performance of the Runtime API while processing an individual HTTP request.

Here's how you can do that:

1. In your `.env` file, set `IS_PROFILING_ENABLED` to `true`
2. Start/restart your development stack: `$ make up-dev`
3. Ensure the endpoint function whose performance you want to profile is defined using `async def` (as opposed to just `def`) ([reference](https://github.com/joerick/pyinstrument/issues/257))

Then—with all of that done—submit an HTTP request that includes the URL query parameter: `profile=true`. Instructions for doing that are in the sections below.

<details>
<summary>Show/hide instructions for <code>GET</code> requests only (involves web browser)</summary>

1. In your web browser, visit the endpoint's URL, but add the `profile=true` query parameter to the URL. Examples:
   ```diff
   A. If the URL doesn't already have query parameters, append `?profile=true`.
   - http://127.0.0.1:8000/nmdcschema/biosample_set
   + http://127.0.0.1:8000/nmdcschema/biosample_set?profile=true

   B. If the URL already has query parameters, append `&profile=true`.
   - http://127.0.0.1:8000/nmdcschema/biosample_set?filter={}
   + http://127.0.0.1:8000/nmdcschema/biosample_set?filter={}&profile=true
   ```
2. Your web browser will display a performance profiling report.
   > Note: The Runtime API will have responded with a performance profiling report web page, instead of its normal response (which the Runtime discards).

That'll only work for `GET` requests, though, since you're limited to specifying the request via the address bar.

</details>

<details>
<summary>Show/hide instructions for <strong>all</strong> kinds of requests (involves <code>curl</code> + web browser)</summary>

1. At your terminal, type or paste the `curl` command you want to run (you can copy/paste one from Swagger UI).
2. Append the `profile=true` query parameter to the URL in the command, and use the `-o` option to save the response to a file whose name ends with `.html`. For example:
   ```diff
     curl -X 'POST' \
   -   'http://127.0.0.1:8000/metadata/json:validate' \
   +   'http://127.0.0.1:8000/metadata/json:validate?profile=true' \
   +    -o /tmp/profile.html
        -H 'accept: application/json' \
        -H 'Content-Type: application/json' \
        -d '{"biosample_set": []}'
   ```
3. Run the command.
   > Note: The Runtime API will respond with a performance profiling report web page, instead of its normal response (which the Runtime discards). The performance profiling report web page will be saved to the `.html` file to which you redirected the command output.
4. Double-click on the `.html` file to view it in your web browser.
   1. Alternatively, open your web browser and navigate to the `.html` file; e.g., enter `file:///tmp/profile.html` into the address bar.

</details>

### RAM usage

The `dagster-daemon` and `dagster-dagit` containers can consume a lot of RAM. If tests are failing and the console of
the `test` container shows "Error 137," here is something you can try as a workaround: In Docker Desktop, go to 
"Settings > Resources > Advanced," and increase the memory limit. One of our team members has
found **12 GB** to be sufficient for running the tests.

> Dedicating 12 GB of RAM to Docker may be prohibitive for some prospective developers.
> There is an open [issue](https://github.com/microbiomedata/nmdc-runtime/issues/928) about the memory requirement.

## Publish to PyPI

This repository contains a GitHub Actions workflow that publishes a Python package to [PyPI](https://pypi.org/project/nmdc-runtime/).

## Links

Here are links related to this repository:

- Production API server: https://api.microbiomedata.org
- PyPI package: https://pypi.org/project/nmdc-runtime
- Container image (API server): https://github.com/microbiomedata/nmdc-runtime/pkgs/container/nmdc-runtime-fastapi
- Container image (Dagster): https://github.com/microbiomedata/nmdc-runtime/pkgs/container/nmdc-runtime-dagster
