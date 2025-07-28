// Note: The `use("nmdc");` syntax works in MongoDB Compass, but not in Studio 3T.
//       The `use nmdc;` syntax works in Studio 3T, but not in MongoDB Compass.
use("nmdc");

/**
 * This MongoDB shell script updates the `jobs` collection so that any
 * documents where `config.was_informed_by` is a string are updated so that
 * `config.was_informed_by` is a single-element array containing that string.
 */

// 🔍 Define the filter for the documents we will be updating.
//
//    Note: The condition — `{ $type: "string" }` — will match documents where either
//          (a) the value of `config.was_informed_by` is a string, or
//          (b) the value is an array that contains a string.
//          Since we don't want to match documents in (b), we include
//          the additional condition — `$not: { $type: "array" } }`.
//          Reference: https://www.mongodb.com/docs/manual/reference/operator/query/type/#querying-by-data-type
//
const filter = { "config.was_informed_by": { $type: "string", $not: { $type: "array" } } };

// 🧮 Before: Print the total number of documents.
let numDocsTotal = db.getCollection("jobs").countDocuments({});
console.log(`Number of documents: ${numDocsTotal}`);

// 🧮 Before: Print the number of documents that match the filter.
let numDocsMatchingFilter = db.getCollection("jobs").countDocuments(filter);
console.log(`Number of documents matching filter: ${numDocsMatchingFilter}`);

// ✏️ Update the documents that match the filter.
//    Reference: https://www.mongodb.com/docs/manual/reference/method/db.collection.updateMany/
db.jobs.updateMany(filter, [
  {
    $set: {
      "config.was_informed_by": ["$config.was_informed_by"],
    },
  }
]);

// 🧮 After: Print the total number of documents.
numDocsTotal = db.getCollection("jobs").countDocuments({});
console.log(`Number of documents (expecting same as before): ${numDocsTotal}`);

// 🧮 After: Print the number of documents that match the filter.
numDocsMatchingFilter = db.getCollection("jobs").countDocuments(filter);
console.log(`Number of documents matching filter (expecting 0): ${numDocsMatchingFilter}`);
