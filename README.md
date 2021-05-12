A runtime system for NMDC data management and orchestration.

## How It Fits In

* [nmdc-schema](https://github.com/microbiomedata/nmdc-schema/)
houses the LinkML schema specification, as well as generated artifacts (e.g. JSON Schema).

* [nmdc-metadata](https://github.com/microbiomedata/nmdc-metadata)
houses code that takes source data and computed data,
and transforms it to broadly accommodate downstream applications such as the data portal.

* [nmdc-server](https://github.com/microbiomedata/nmdc-server)
houses code specific to the data portal -- its database, back-end API, and front-end application.

* [workflow_documentation](https://nmdc-workflow-documentation.readthedocs.io/en/latest/index.html)
references workflow code spread across several repositories, that take source data and produce computed data.

* This repo (nmdc-runtime) manages execution of lightweight data transformations (e.g. in nmdc-metadata) and of workflows,
including ensuring that spawned processes have access to needed configuration and data resources.

## Overview

The runtime features:

1. [Dagster](https://docs.dagster.io/concepts) orchestration:
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
      configuration. There are TerminusDB and MongoDB `resources` defined, as well as `preset`
      configuration definitions for both "dev" and "prod" `modes`. The `preset`s tell Dagster to
      look to a set of known environment variables to load resources configurations, depending on
      the `mode`.

2. A [TerminusDB](https://terminusdb.com/) database supporting revision control of schema-validated
data.
   
3. A [MongoDB](https://www.mongodb.com/) database supporting write-once, high-throughput internal
data storage by the nmdc-runtime FastAPI instance.
   
4. A [FastAPI](https://fastapi.tiangolo.com/) service to interface with the orchestrator and
database, as a hub for data management and workflow automation.

## Local Development

Ensure Docker (and Docker Compose) are installed.

```bash
# optional: copy .env.dev to .env (gitignore'd) and set those vars
make up-dev
```

Docker Compose is used to start local TerminusDB, MongoDB, and PostgresSQL (used by Dagster to log
information) instances, as well as a Dagster web server (dagit) and daemon (dagster-daemon).

The Dagit web server is viewable at http://localhost:3000/.

The TerminusDB browser console is viewable at http://localhost:6364/.

The FastAPI service is viewable at http://localhost:8000/ -- e.g., rendered documentation at
http://localhost:8000/redoc/.

## Local Testing

Tests can be found in `tests` and are run with the following command:

```bash
pytest tests
```

As you create Dagster solids and pipelines, add tests in `tests/` to check that your
code behaves as desired and does not break over time.

[For hints on how to write tests for solids and pipelines in Dagster, see their documentation tutorial on Testing](https://docs.dagster.io/tutorial/testable).
