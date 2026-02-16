#! /bin/sh

###############################################################################
# Overview
# --------
# This shell script uses `mongosh` to check whether the specified MongoDB
# instance is a member of a replica set and the replica set is healthy.
#
# This shell script was designed to be run within a Docker container based upon
# the `mongo:8.2.3` container image (i.e., https://hub.docker.com/_/mongo).
#
# References
# ----------
# 1. https://www.mongodb.com/docs/manual/reference/method/rs.status/
# 2. https://www.mongodb.com/docs/mongodb-shell/reference/options/
# 3. https://www.warp.dev/terminus/docker-compose-health-check
###############################################################################

# If the replica set is OK, exit with a status of 0. Otherwise, exit with a status of 1.
#
# Note: The exit code of the final command in this shell script will become the exit code
#       of the shell script, itself.
#
# Note: Failing to catch an exception would also exit with a status of 1.
#       We opted to catch the exception ourselves to make the code more self-documenting.
#
mongosh --quiet --eval '
  try {
    if (rs.status().ok === 1) {
      quit(0);
    }

    quit(1);
  } catch {
    quit(1);
  }
'