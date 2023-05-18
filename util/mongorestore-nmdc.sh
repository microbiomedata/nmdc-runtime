#!/bin/bash
# Execute from repo root dir:
# $  ./util/mongorestore-nmdc.sh
mongorestore -h $MONGO_HOST -u $MONGO_USERNAME -p $MONGO_PASSWORD --authenticationDatabase=admin \
     --gzip --drop \
    $HOME/nmdcdb-mongodump/nmdcdb/2023-05-17T15/