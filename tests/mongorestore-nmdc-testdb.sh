#!/bin/bash
# Execute from repo root dir:
# $  ./tests/mongorestore-nmdc-testdb.sh
mongorestore -h $MONGO_HOST -u $MONGO_USERNAME -p $MONGO_PASSWORD --authenticationDatabase=admin \
    --gzip --nsInclude='${MONGO_DBNAME}.*' --drop \
    --nsFrom='${MONGO_DBNAME}.$name$' --nsTo='nmdc.$name$' \
    tests/nmdcdb/

## restore selected collections to target
#mongorestore -h $MONGO_HOST -u $MONGO_USERNAME -p $MONGO_PASSWORD --authenticationDatabase=admin \
#    --gzip --nsInclude='nmdc-runtime-admin.*' --drop \
#    --nsFrom='nmdc-runtime-admin.$name$' --nsTo='nmdc-next.$name$' \
#    tests/nmdcdb/