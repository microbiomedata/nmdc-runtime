#!/bin/bash
# Execute from repo root dir:
# $  ./tests/mongodump-nmdc-testdb.sh
mongodump -h $MONGO_HOST -u $MONGO_USERNAME -p $MONGO_PASSWORD --authenticationDatabase=admin \
    -d $MONGO_DBNAME \
    --gzip -o tests/nmdcdb/ \
    --excludeCollectionsWithPrefix="functional_annotation_"