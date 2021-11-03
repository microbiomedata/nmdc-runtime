#!/bin/bash
# Execute from repo root dir:
# $  ./tests/mongorestore-nmdc-testdb.sh
mongorestore $MONGO_HOST -u $MONGO_USERNAME -p $MONGO_PASSWORD --authenticationDatabase=admin \
    --gzip --archive=tests/nmdcdb.test.archive.gz --nsInclude="${MONGO_DBNAME}.*" --drop