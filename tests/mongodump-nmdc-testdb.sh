#!/bin/bash
# Execute from repo root dir:
# $  ./tests/mongodump-nmdc-testdb.sh
mongodump -u $MONGODB_ROOT_USER -p $MONGODB_ROOT_PASSWORD --authenticationDatabase=admin \
    -d nmdc \
    --gzip -o /app_tests/nmdcdb/