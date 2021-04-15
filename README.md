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
    - `solids` represent individual units of computation
    - `pipelines` are built up from solids
    - `schedules` trigger recurring pipeline runs based on time
    - `sensors` trigger pipeline runs based on external state

2. A [TerminusDB](https://terminusdb.com/) database supporting revision control of schema-validated
data.
   
3. A [FastAPI](https://fastapi.tiangolo.com/) service to interface with the orchestrator and
database.

## Local Development

Use Docker Compose. This will start a Dagit web
server that, by default, is viewable at http://localhost:3000.

```bash
cd nmdc_runtime
docker-compose up
```

If you change the local repository code, be sure to rebuild the container images:
```bash
docker-compose down
docker-compose up --build --force-recreate
```

## Local Testing

Tests can be found in `tests` and are run with the following command:

```bash
pytest tests
```

As you create Dagster solids and pipelines, add tests in `tests/` to check that your
code behaves as desired and does not break over time.

[For hints on how to write tests for solids and pipelines in Dagster, see their documentation tutorial on Testing](https://docs.dagster.io/tutorial/testable).
