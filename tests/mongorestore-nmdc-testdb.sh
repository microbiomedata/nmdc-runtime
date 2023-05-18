#!/bin/bash
# Execute from repo root dir:
# $  ./tests/mongorestore-nmdc-testdb.sh
mongorestore -u $MONGODB_ROOT_USER -p $MONGODB_ROOT_PASSWORD --authenticationDatabase=admin \
    --gzip --drop \
    /nmdc_dump/nmdcdb/2023-05-17T16/