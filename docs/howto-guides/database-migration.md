# How to migrate the database

## Introduction

In this [how-to guide](https://diataxis.fr/how-to-guides/), I'll tell you how you can migrate the Mongo database from one NMDC Schema version to another. Also, when telling you about a step that has a shortcoming I'm aware of, I'll point that out.

## Glossary

- **Mongo**: A nickname for MongoDB.
- **Old schema**: The version of the NMDC Schema you will be migrating the database **from**.
- **New schema**: The version of the NMDC Schema you will be migrating the database **to**.
- **Origin database**: The database you want to migrate.
- **Transformation database**: The database you will use to transform data.

## Prerequisites

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

1. Make the **origin database** be **read-only** for all users except the `root` user.
    1. Connect to the Mongo server containing the origin database.
       ```shell
       mongosh "mongodb://localhost:10000" --username root --authenticationDatabase admin
       password: ********
       ```
    1. At the Mongo Shell, display the username and role of each user.
       ```js
       use admin;
       db.getCollection("system.users").find({}, { _id: 0, user: 1, roles: 1 });
       // --> [ { user: 'alice', roles: [ { role: 'readWrite', db: 'nmdc' } ] }, ... ]
       ```
       Copy/paste the output into a text file.
    1. For every user that has the `readWrite` role on the `nmdc` database, change that role to `read`:
       ```js
       // TODO: Provide a mongosh command for this. Ensure it doesn't affect users' roles on other databases.
       ```
    > #### Shortcomings
    > - This doesn't account for [user-defined roles](https://www.mongodb.com/docs/v6.0/core/security-user-defined-roles/), which could have a name other than `readWrite` while still providing write access to the `nmdc` database.
    > - This doesn't account for the [built-in roles](https://www.mongodb.com/docs/v6.0/reference/built-in-roles/)—other than `root`—that provide modification access to the `nmdc` database (e.g. `dbAdmin`, `dbOwner`, etc.).

## Appendix

### Precursors to this how-to guide

- The "Data Releases" section of [`docs/howto-buides/release-process.md`](./release-process.md)
- The [Jupyter notebook](../../demo/metadata_migration/from_7_7_2_to_7_8_0/migrate_7_7_2_to_7_8_0.ipynb) used for the `nmdc-schema` version `7.7.2` -> `7.8.0` migration

