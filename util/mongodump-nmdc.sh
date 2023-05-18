#!/bin/bash
# Execute from repo root dir:
# $  ./util/mongodump-nmdc.sh
mongodump -h $MONGO_HOST -u $MONGO_USERNAME -p $MONGO_PASSWORD --authenticationDatabase=admin \
    -d $MONGO_DBNAME \
    --gzip -o $HOME/nmdcdb-mongodump/nmdcdb/$(date +"%Y-%m-%dT%H")/