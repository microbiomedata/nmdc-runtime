// A one-time script that distributes documents from the single `_runtime.analytics` Mongo
// collection (which is slow to work with due to its size) into multiple year-and-month-specific
// collections (i.e. `_runtime.analytics.<YYYY>_<MM>`; e.g. `_runtime.analytics.2026_11`),
// based upon each document's `created_at` value.
//
// Prerequisites:
// - `mongosh` is installed (alternatively, connect MongoDB Compass to the relevant Mongo database
//   and then use the "MongoDB Shell" embedded in the MongoDB Compass GUI)
//
// Optional pre-run optimization:
// - Consider creating an index on the "created_at" field in the "_runtime.analytics" collection
//   before running this script, so that the queries this script performs on that collection
//   are faster.
//
// Usage:
//   $ mongosh --host "$MONGO_HOST" --port "$MONGO_PORT" \
//       --username "$MONGO_USERNAME" --password "$MONGO_PASSWORD" \
//       --authenticationDatabase admin \
//       --file db/migrations/ad_hoc_scripts/distribute_analytics_into_yyyy_mm_collections.mongosh.js
//
// Notes:
// 1. Because this script uses `whenMatched: "keepExisting"`, you can safely run it multiple times
//    and it won't duplicate documents in the "YYYY_MM" collections.
// 2. Unless you set `DROP_SOURCE_COLLECTION = true` (see below), the script will leave the original
//    collection intact. So, you can perform dry run(s) and examine the results before you drop it.

// Note: The `use("nmdc");` syntax works in `mongosh` and MongoDB Compass,
//       whereas Studio 3T expects `use nmdc;`.
use("nmdc");

// TODO: If you want this script to drop the `_runtime.analytics` collection, set this to `true`;
//       otherwise, leave it at `false`.
const DROP_SOURCE_COLLECTION = false;

// ---------------------------------------------------------------------------
// Step 1: Check the source collection and validate "created_at" timestamp prefixes
// ---------------------------------------------------------------------------
const sourceCollection = db.getCollection("_runtime.analytics");
const numSourceDocuments = sourceCollection.countDocuments({});
console.log(`Collection '_runtime.analytics' contains ${numSourceDocuments} documents.`);

// Confirm every document has a "created_at" field whose value begins with `YYYY-MM` (e.g. "2026-11").
// Later, we'll use that prefix to put that document into the correct year/month-specific collection.
const numInvalidSourceDocuments = sourceCollection.countDocuments({
  $or: [
    { created_at: { $exists: false } },
    { created_at: { $not: { $type: "string" } } },
    { created_at: { $not: /^\d{4}-(0[1-9]|1[0-2])/ } },
  ],
});
if (numInvalidSourceDocuments > 0) {
  throw new Error(
    `Found ${numInvalidSourceDocuments} documents in '_runtime.analytics' lacking a created_at ` +
    `field containing a string that begins with 'YYYY-MM...'. Resolve that, then retry.`
  );
}

// ---------------------------------------------------------------------------
// Step 2: Determine all distinct `YYYY-MM` prefixes in `_runtime.analytics`
// ---------------------------------------------------------------------------
// Extract the first 7 characters ("YYYY-MM") of each ISO-8601 `created_at` string,
// group them, and make a sorted list of distinct year-and-month combinations.
console.log("Discovering distinct YYYY-MM values...");

const distinctYearMonthCombinations = sourceCollection
  .aggregate([
    { $project: { _id: 0, yearDashMonth: { $substrBytes: ["$created_at", 0, 7] } } },
    { $group: { _id: "$yearDashMonth" } },
    { $sort: { _id: 1 } },
  ])
  .toArray()
  .map((doc) => doc._id);

console.log(`Found ${distinctYearMonthCombinations.length} month values: ${distinctYearMonthCombinations.join(", ")}`);

// ---------------------------------------------------------------------------
// Step 3: Copy each month's documents into the respective
//         `_runtime.analytics.<YYYY>_<MM>` collection
// ---------------------------------------------------------------------------
for (const yearDashMonth of distinctYearMonthCombinations) {
  const [yyyyStr, mmStr] = yearDashMonth.split("-");
  const targetYearMonthCollection = `_runtime.analytics.${yyyyStr}_${mmStr}`;

  // Build an exclusive upper bound for the month (e.g. "2026-11" -> "2026-12",
  // "2026-12" -> "2027-01") so all ISO timestamps in that month match cleanly.
  const year = parseInt(yyyyStr, 10);
  const month = parseInt(mmStr, 10);
  const nextYYYY = month === 12 ? year + 1 : year; // we'll assume this is 4 digits
  const nextMMStr = String(month === 12 ? 1 : month + 1).padStart(2, "0");
  const nextYearDashMonth = `${nextYYYY}-${nextMMStr}`;

  // As a reminder, comparing YYYY-MM alphanumerically produces the same results as
  // comparing them chronologically (e.g. "2024-09" < "2024-10").
  const filterCreatedAtWithinRange = { created_at: { $gte: yearDashMonth, $lt: nextYearDashMonth } };
  const numMatchingSourceDocuments = sourceCollection.countDocuments(filterCreatedAtWithinRange);

  console.log(
    `Copying ${numMatchingSourceDocuments} (${yearDashMonth}) documents ` +
    `into year/month-specific collection '${targetYearMonthCollection}'...`
  );
  sourceCollection.aggregate([
    { $match: filterCreatedAtWithinRange },
    {
      $merge: {
        into: targetYearMonthCollection,
        on: "_id",
        whenMatched: "keepExisting", // makes our script safe to re-run
        whenNotMatched: "insert",
      },
    },
  ]);

  const numDocumentsInTargetYearMonthCollection = db.getCollection(targetYearMonthCollection).countDocuments({});
  console.log(
    `Year/month-specific collection '${targetYearMonthCollection}' now contains ` +
    `${numDocumentsInTargetYearMonthCollection} documents.`
  );
  if (numDocumentsInTargetYearMonthCollection < numMatchingSourceDocuments) {
    throw new Error(
      "Number of documents in year/month-specific collection does not match expectation."
    );
  }
}

// ---------------------------------------------------------------------------
// Step 4: Optionally, drop the `_runtime.analytics` collection
// ---------------------------------------------------------------------------
if (DROP_SOURCE_COLLECTION) {
  console.log(`Dropping '_runtime.analytics' collection...`);
  sourceCollection.drop();
  console.log(`Dropped '_runtime.analytics' collection.`);
} else {
  console.log("Not dropping '_runtime.analytics' collection.");
}
