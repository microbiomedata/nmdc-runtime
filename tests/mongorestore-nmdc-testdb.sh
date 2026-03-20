#!/bin/bash

# Summary: This script restores a pre-determined MongoDB database dump into the MongoDB server at `localhost:27017`.

# Example usage from repo root dir:
# $ docker compose up mongo --force-recreate --detach
# $ docker compose exec mongo /bin/bash -c "/mongorestore-nmdc-testdb.sh"

# Build authentication-related CLI options, based upon environment variables.
# Note: We use an array here to avoid issues—when the values contain spaces or
#       quotes—that can arise when accumulating CLI options into a _string_.
AUTH_OPTIONS=("--authenticationDatabase" "admin")
if [ -n "${MONGO_USERNAME}" ]; then
     AUTH_OPTIONS+=("-u" "${MONGO_USERNAME}")
fi
if [ -n "${MONGO_PASSWORD}" ]; then
     AUTH_OPTIONS+=("-p" "${MONGO_PASSWORD}")
fi

mongorestore "${AUTH_OPTIONS[@]}" --gzip --drop /nmdcdb_dump/dump_nmdc-prod_2025-02-12_20-12-02