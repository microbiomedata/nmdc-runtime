#!/bin/bash

# Summary: This script restores a pre-determined MongoDB database dump into the MongoDB server at `localhost:27017`.

# Example usage from repo root dir:
# $ docker compose up mongo --force-recreate --detach
# $ docker compose exec mongo /bin/bash -c "/mongorestore-nmdc-testdb.sh"

# Build authentication-related CLI options, based upon environment variables.
AUTH_OPTIONS=""
if [ -n "$MONGO_INITDB_ROOT_USERNAME" ] && [ -n "$MONGO_INITDB_ROOT_PASSWORD" ]; then
    AUTH_OPTIONS="-u '$MONGO_INITDB_ROOT_USERNAME' -p '$MONGO_INITDB_ROOT_PASSWORD' --authenticationDatabase admin"
fi

mongorestore $AUTH_OPTIONS --gzip --drop /nmdcdb_dump/dump_nmdc-prod_2025-02-12_20-12-02