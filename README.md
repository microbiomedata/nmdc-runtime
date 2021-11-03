A runtime system for NMDC data management and orchestration.

## How It Fits In

* [nmdc-metadata](https://github.com/microbiomedata/nmdc-metadata)
tracks issues related to NMDC metadata, which may necessitate work across multiple repos.
  
* [nmdc-schema](https://github.com/microbiomedata/nmdc-schema/)
houses the LinkML schema specification, as well as generated artifacts (e.g. JSON Schema).

* [nmdc-server](https://github.com/microbiomedata/nmdc-server)
houses code specific to the data portal -- its database, back-end API, and front-end application.

* [workflow_documentation](https://nmdc-workflow-documentation.readthedocs.io/en/latest/index.html)
references workflow code spread across several repositories, that take source data and produce computed data.

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
         configuration. There are TerminusDB and MongoDB `resources` defined, as well as `preset`
         configuration definitions for both "dev" and "prod" `modes`. The `preset`s tell Dagster to
         look to a set of known environment variables to load resources configurations, depending on
         the `mode`.

2. A [TerminusDB](https://terminusdb.com/) database supporting revision control of schema-validated
data.
   
3. A MongoDB database supporting write-once, high-throughput internal
data storage by the nmdc-runtime FastAPI instance.
   
4. A [FastAPI](https://fastapi.tiangolo.com/) service to interface with the orchestrator and
database, as a hub for data management and workflow automation.

## Local Development

Ensure Docker (and Docker Compose) are installed.

```bash
# optional: copy .env.example to .env (gitignore'd) and set those vars
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
make test
```

As you create Dagster solids and pipelines, add tests in `tests/` to check that your
code behaves as desired and does not break over time.

[For hints on how to write tests for solids and pipelines in Dagster, see their documentation tutorial on Testing](https://docs.dagster.io/tutorial/testable).

