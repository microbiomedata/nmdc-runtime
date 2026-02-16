#!/bin/bash
# Execute from repo root dir:
# $ export $(grep -v '^#' .env.localhost | xargs)
# $ ./util/mongorestore-nmdc.sh

# Build authentication-related CLI options, based upon environment variables.
AUTH_OPTIONS=""
if [ -n "$MONGO_USERNAME" ] && [ -n "$MONGO_PASSWORD" ]; then
    AUTH_OPTIONS="-u '$MONGO_USERNAME' -p '$MONGO_PASSWORD' --authenticationDatabase admin"
fi

mongorestore -h $MONGO_HOST $AUTH_OPTIONS \
     --gzip --drop \
    $HOME/nmdcdb-mongodump/nmdcdb/2024-07-29_20-12-07/