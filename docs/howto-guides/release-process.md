# Release Process

## NMDC Runtime Releases
How do new versions of the API and NMDC Runtime site (Dagster daemon and Dagit frontend) get
released? Here's how.

1. Ensure the tests pass (i.e., a "smoke test").
    
    - Either run tests locally
    ```
    make up-test
    make test
    ```
   - or confirm the test pass via 
     [our python-app.yml GitHub
     action](https://github.com/microbiomedata/nmdc-runtime/blob/main/.github/workflows/python-app.yml),
     which is triggered automatically when a change to any Python file in the repository is pushed to the
     `main` branch, or to a Pull Request. You can monitor the status of Github Actions
     [here](https://github.com/microbiomedata/nmdc-runtime/actions).

2. Add a summary for the release to `RELEASES.md`. You can make an edit and push to the `main`
   branch [via GitHub](https://github.com/microbiomedata/nmdc-runtime/blob/main/RELEASES.md). This will
   trigger two GitHub actions in sequence to

       - [build and push updated Docker images](https://github.com/microbiomedata/nmdc-runtime/blob/main/.github/workflows/build-and-push-docker-images.yml) for the API server and for the NMDC Runtime site's Dagster daemon and Dagit dashboard, and

       - [deploy the new images to Spin](https://github.com/microbiomedata/nmdc-runtime/blob/main/.github/workflows/release-to-spin.yml).


## Data Releases
In order to make sure the schema, database, and NMDC Runtime API are in sync we need to coordinate data updates that require schema changes. 

Here is a summary of the process:
1. [NMDC Schema](https://github.com/microbiomedata/nmdc-schema) repo releases new version. All releases must include a migration script (even if it is null / empty) to run against MongoDB. See ADR 007
2. Build a new NMDC-runtime image so that it is ready to be deployed (See above). 
3. Database (Mongo) is switched to read-only mode to prevent inconsistencies.
     - TODO: decide on process for read-only mode)
4. Run `mongodump` to dump database on local machine
     - TODO: document mongodump command
     - FUTURE: improved process for doing inline DB migrations
5. Run migration script runs against DB dump to perform conversions
     - TODO: Finalize location and instructions for migration script
6. Run validation to make sure new DB is consistent
     - TODO: Steps for validation
7. If validation succeeds run `mongorestore` to update database
     - TODO: Steps for Mongorestore
9. Upgrade NMDC-runtime repo to latest version in Spin





