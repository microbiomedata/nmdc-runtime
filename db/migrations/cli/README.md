# CLI

Command-Line Interface (CLI) application that you can use to migrate the NMDC database
between two versions of the NMDC schema.

This app was designed to be run in an environment having the following:

1. Read and write access to the original MongoDB database
2. Root access to a distinct "transformer" MongoDB database
3. The programs: `mongosh`, `mongodump`, and `mongorestore`

In practice, we deploy it as follows:

1. In `nmdc-runtime` (here), we have this CLI app. Although it resides in the same repo as the
   Runtime, they are two independent applications and have two independent `pyproject.toml` files.
   This CLI app could be moved to a separate repository without the Runtime realizing it.
2. In `nmdc-cloud-deployment` (private), we have a normally-suspended CronJob that uses the official
   [mongo](https://hub.docker.com/_/mongo) container image. The CronJob supplies the container with
   a [startup script](https://hub.docker.com/_/mongo#initializing-a-fresh-instance) that installs
   the dependencies of this CLI app and then runs this CLI app. That startup script resides in the
   `nmdc-cloud-deployment` repo.
