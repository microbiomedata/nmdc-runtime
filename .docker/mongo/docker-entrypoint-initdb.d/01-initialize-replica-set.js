/**
 * Overview
 * --------
 * This JavaScript script uses `mongosh` to: (a) check whether the specified MongoDB
 * instance is a member of a replica set and, if is isn't, (b) creates a replica
 * set of which that MongoDB instance is the sole member.
 * 
 * This JavaScript script was designed to be run within a Docker container based upon
 * the `mongo:8.2.3` container image (i.e., https://hub.docker.com/_/mongo).
 * Docs: https://hub.docker.com/_/mongo#initializing-a-fresh-instance
 * 
 * Running MongoDB in replica set mode is necessary in order to use MongoDB transactions.
 * 
 * Reference: https://www.mongodb.com/docs/manual/reference/method/rs.status/
 */

try {
  print("Checking whether replica set has been initialized.");
  rs.status();
  print("Replica set has already been initialized.");
} catch {
  print("Initializing replica set.");
  rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "localhost" }] });
  print("Replica set initialized.");
}
