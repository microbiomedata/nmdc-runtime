#! /bin/sh

###############################################################################
# Overview
# --------
# This shell script uses `mongosh` to: (a) check whether the local MongoDB
# instance is a member of a replica set and, if is isn't, (b) creates a replica
# set of which the local MongoDB instance is the sole member.
#
# This shell script was designed to be run within a Docker container based upon
# the `mongo:7.0.15` container image (i.e., https://hub.docker.com/_/mongo).
#
# References
# ----------
# 1. https://www.mongodb.com/docs/manual/reference/method/rs.status/
# 2. https://www.mongodb.com/docs/mongodb-shell/reference/options/
# 3. https://www.warp.dev/terminus/docker-compose-health-check
###############################################################################

echo '
  try {
    // Note: If the replica set is not initiated yet, the invoked function will
    //       throw an error, which will be caught below.
    rs.status();
  } catch (e) {
    rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "localhost" }] });
  }
' | mongosh \
  --username "${MONGO_INITDB_ROOT_USERNAME}" \
  --password "${MONGO_INITDB_ROOT_PASSWORD}"
