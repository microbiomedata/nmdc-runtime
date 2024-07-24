#!/bin/bash
# Execute from repo root dir:
# $ export $(grep -v '^#' .env.localhost.prod | xargs)
# $ ./util/mongodump-nmdc.sh
mongodump -v -h $MONGO_HOST -u $MONGO_USERNAME -p $MONGO_PASSWORD --authenticationDatabase=admin \
    -d $MONGO_DBNAME \
    --gzip -o $HOME/nmdcdb-mongodump/nmdcdb/$(date +"%Y-%m-%dT%H")/ \
    --excludeCollectionsWithPrefix="_runtime" \
    --excludeCollectionsWithPrefix="_tmp" \
    --excludeCollectionsWithPrefix="fs." \
    --excludeCollectionsWithPrefix="ids_" \
    --excludeCollectionsWithPrefix="txn_log"