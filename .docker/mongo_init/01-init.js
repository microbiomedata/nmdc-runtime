/**
 * This JavaScript script creates a replica set of which the MongoDB instance
 * running the script is the role member.
 * 
 * Note: Running in replica set mode is necessary in order to use MongoDB transactions.
 * 
 * Reference: https://www.mongodb.com/docs/manual/reference/method/rs.status/
 */

  try {
    print("Checking whether replica set has been initialized.")
    rs.status();
    print("Replica set has already been initialized.")
  } catch (e) {
    print("Initializing replica set.")
    rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "localhost" }] });
    print("Replica set initialized.")
  }
