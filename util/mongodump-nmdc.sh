#!/bin/bash
# Execute from repo root dir:
# $ export $(grep -v '^#' .env.localhost.prod | xargs)
# $ ./util/mongodump-nmdc.sh

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

mongodump -v -h "${MONGO_HOST}" "${AUTH_OPTIONS[@]}" \
    -d "${MONGO_DBNAME}" \
    --gzip -o $HOME/nmdcdb-mongodump/nmdcdb/$(date +"%Y-%m-%dT%H")/ \
    --excludeCollectionsWithPrefix="_runtime" \
    --excludeCollectionsWithPrefix="_tmp" \
    --excludeCollectionsWithPrefix="fs." \
    --excludeCollectionsWithPrefix="ids_" \
    --excludeCollectionsWithPrefix="txn_log"