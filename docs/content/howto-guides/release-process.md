# Release Process

## NMDC Runtime Releases
How do new versions of the API and NMDC Runtime site (Dagster daemon and Dagit frontend) get
released? See [infra-admin](https://github.com/microbiomedata/infra-admin/blob/main/releases/nmdc-runtime.md) for details (private to `microbiomedata` org).


## Data Releases
In order to make sure the schema, database, and NMDC Runtime API are in sync we need to coordinate data updates that require schema changes. 

Here is a summary of the process:

1. [NMDC Schema](https://github.com/microbiomedata/nmdc-schema) repo releases new version. All releases must include a migration script (even if it is null / empty) to run against MongoDB. See [ADR 007](https://github.com/microbiomedata/issues/blob/main/decisions/0007-mongo-migration-scripts.md)
2. Submit/Merge a PR with updated schema version and any related code changes.
3. Build a new NMDC-runtime image so that it is ready to be deployed (See above). 
4. Database (Mongo) is switched to read-only mode to prevent inconsistencies.
     - TODO: decide on process for read-only mode
5. Run `mongodump` to dump database on local machine
     - TODO: document `mongodump` command
     - FUTURE: improved process for doing inline DB migrations
6. Run migration script runs against database on local machine (to migrate data)
     - TODO: Finalize location and instructions for migration script
7. Run validation to make sure database on local machine adheres to updated schema version
     - TODO: Steps for validation
8. If validation succeeds, run `mongorestore` to update database
     - TODO: Steps for `mongorestore`
9. Database (Mongo) is switched from read-only mode back to original mode.
10. Upgrade NMDC-runtime repo to latest version in Spin
