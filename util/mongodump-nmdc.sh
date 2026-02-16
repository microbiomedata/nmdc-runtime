#!/bin/bash
# Execute from repo root dir:
# $ export $(grep -v '^#' .env.localhost.prod | xargs)
# $ ./util/mongodump-nmdc.sh

# Build authentication-related CLI options, based upon environment variables.
AUTH_OPTIONS=""
if [ -n "$MONGO_USERNAME" ] && [ -n "$MONGO_PASSWORD" ]; then
    AUTH_OPTIONS="-u '$MONGO_USERNAME' -p '$MONGO_PASSWORD' --authenticationDatabase admin"
fi

mongodump -v -h $MONGO_HOST $AUTH_OPTIONS \
    -d $MONGO_DBNAME \
    --gzip -o $HOME/nmdcdb-mongodump/nmdcdb/$(date +"%Y-%m-%dT%H")/ \
    --excludeCollectionsWithPrefix="_runtime" \
    --excludeCollectionsWithPrefix="_tmp" \
    --excludeCollectionsWithPrefix="fs." \
    --excludeCollectionsWithPrefix="ids_" \
    --excludeCollectionsWithPrefix="txn_log"