#!/bin/bash

# Summary: This script restores a pre-determined MongoDB database dump into the MongoDB server at `localhost:27017`.

# Example usage from repo root dir:
# $ docker compose up mongo --force-recreate --detach
# $ docker compose exec mongo /bin/bash -c "/mongorestore-nmdc-testdb.sh"
mongorestore -u $MONGO_INITDB_ROOT_USERNAME -p $MONGO_INITDB_ROOT_PASSWORD --authenticationDatabase=admin \
    --gzip --drop /nmdcdb_dump/dump_nmdc-prod_2025-02-12_20-12-02