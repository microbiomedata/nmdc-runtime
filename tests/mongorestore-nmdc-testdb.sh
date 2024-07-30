#!/bin/bash
# Execute from repo root dir:
# $  ./tests/mongorestore-nmdc-testdb.sh
mongorestore -u $MONGO_INITDB_ROOT_USERNAME -p $MONGO_INITDB_ROOT_PASSWORD --authenticationDatabase=admin \
    --gzip --drop \
    /nmdc_dump/nmdcdb/2024-07-30T11/