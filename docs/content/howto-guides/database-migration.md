# How to migrate the database

## Introduction

In this [how-to guide](https://diataxis.fr/how-to-guides/), I'll tell you how you can migrate the Mongo database from conforming to _one_ version of the NMDC Schema, to conforming to _another_ version of the NMDC Schema.

## Glossary

- **Mongo**: A nickname for MongoDB.
- **Old schema**: The version of the NMDC Schema you will be migrating the database **from**.
- **New schema**: The version of the NMDC Schema you will be migrating the database **to**.
- **Origin database**: The database you want to migrate.
- **Transformation database**: The database you will use to transform data.

## Overview

We currently use an [Extract, Transform, Load](https://en.wikipedia.org/wiki/Extract,_transform,_load) (ETL) process to migrate the database.

We use Jupyter notebooks to **perform** the "Extract" and "Load" steps, and to **orchestrate** the "Transform" step. We use Python scripts to **perform** the "Transform" step.

The Jupyter notebooks reside in the `db/` directory of the `nmdc-runtime` repository. In general, we try to keep all code that interacts directly with the NMDC database, in the `nmdc-runtime` repository.

The Python scripts reside in the `nmdc_schema/migrations` directory of the `nmdc-schema` repository. These are typically written by data modelers.

The basic flow of a migration looks like this:

```mermaid
%% This is a Mermaid diagram.
%% Docs: https://docs.mermaidchart.com/mermaid-oss/syntax/sequenceDiagram.html

sequenceDiagram
    actor USER as Administrator

    box rgba(0, 0, 0, 0.5) Laptop
        participant DB_T as Mongo<br>(transformer)
        participant NB   as Jupyter<br>Notebook
    end

    box rgba(0, 0, 0, 0.5) Production infrastructure
        participant DB_O    as Mongo<br>(origin)
        participant RUNTIME as Runtime
    end

    activate RUNTIME
    USER ->> RUNTIME: Take offline
    deactivate RUNTIME

    USER ->> NB: Run notebook

    activate NB
    NB ->> DB_O: Revoke user/app access
    NB ->> DB_O: Extract data<br>via mongodump
    DB_O -->> NB: 
    NB ->> DB_T: Load data<br>via mongorestore
    activate DB_T
    NB ->> DB_T: Transform data<br>via Python scripts
    NB ->> DB_T: Validate data<br>via LinkML
    NB ->> DB_T: Extract data<br>via mongodump
    DB_T -->> NB: 
    deactivate DB_T
    Note right of NB: Last chance to<br>abort migration
    NB ->> DB_O: Load data<br>via mongorestore
    NB ->> DB_O: Restore user/app access
    deactivate NB

    USER ->> RUNTIME: Bring online (typically a new version, using the new schema)
    activate RUNTIME
```

## Prerequisites

<!-- FIXME: This is at least partially obsolete. -->

1. You're running the latest version of [nmdc-runtime](https://github.com/microbiomedata/nmdc-runtime) on your computer—by that, I mean:
    - Its [Docker-based development environment](https://github.com/microbiomedata/nmdc-runtime/blob/main/docker-compose.yml) is running on your computer (at least, the `fastapi` and `mongo` containers).
    - The `main` branch in your clone matches the `main` branch on GitHub (run `$ git diff main origin/main` to check).
    - The `main` branch is checked out and there are no uncommitted changes (run `$ git status` to check).
    - The `nmdc-schema` Python package used by this version of `nmdc-runtime` contains the **old schema**.
1. An `nmdc-schema` Python package containing the **new schema** is available on [PyPI](https://pypi.org/project/nmdc-schema/).
    - The package version number is stored in [(nmdc-schema) `pyproject.toml`](https://github.com/microbiomedata/nmdc-schema/blob/main/pyproject.toml#L13)
    - The schema version number is stored in [(nmdc-schema) `src/schema/nmdc.yaml`](https://github.com/microbiomedata/nmdc-schema/blob/main/src/schema/nmdc.yaml#L22)
1. Root credentials for the **origin database**.
1. Root credentials for the **transformation database**.

## Procedure

<!-- TODO: Write this. Consider moving the above Mermaid chart to here. -->

1. 

## Appendix

### Precursors to this how-to guide

- The "Data Releases" section of [`docs/howto-guides/release-process.md`](./release-process.md)
