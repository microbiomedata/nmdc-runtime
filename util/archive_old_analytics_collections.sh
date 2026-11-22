#!/bin/bash
set -euo pipefail

# Dump (and, optionally, drop from the database) old `_runtime.analytics.<YYYY>_<MM>` collections
# so they stop taking up space and (more importantly to me) slowing down database dumps during
# our monthly releases.
#
# Prerequisites:
# - `mongosh` and `mongodump` are installed
# - MongoDB-related environment variables are set:
#   `MONGO_HOST`, `MONGO_PORT`, `MONGO_USERNAME`, `MONGO_PASSWORD`, `MONGO_DBNAME`
#
# Usage (from repository root):
#   $ ./util/archive_old_analytics_collections.sh 2026_10 --output-dir /tmp/analytics-archives
#   $ ./util/archive_old_analytics_collections.sh 2026_10 --output-dir /tmp/analytics-archives --drop
#
# Output:
#   Writes gzipped BSON dumps under:
#     <OUTPUT_DIR>/<MONGO_DBNAME>/

# ---------------------------------------------------------------------------
# Step 1: Parse and validate CLI arguments
# ---------------------------------------------------------------------------
if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
    echo "Usage: $0 <YYYY_MM> --output-dir <DIR> [--drop]" >&2
    echo "  Dumps (and optionally drops) all _runtime.analytics.<YYYY>_<MM> collections older than YYYY_MM." >&2
    exit 1
fi

YYYY_MM="$1"
if [[ ! "$YYYY_MM" =~ ^[0-9]{4}_(0[1-9]|1[0-2])$ ]]; then
    echo "Error: Cutoff year_month must be in format YYYY_MM with month 01-12 (e.g. 2026_10), got: '$YYYY_MM'" >&2
    exit 1
fi

if [ "$2" != "--output-dir" ] || [ -z "$3" ]; then
    echo "Usage: $0 <YYYY_MM> --output-dir <DIR> [--drop]" >&2
    exit 1
fi
OUTPUT_DIR="$3"

DROP_AFTER_DUMP=false
if [ "$#" -eq 4 ]; then
    if [ "$4" = "--drop" ]; then
        DROP_AFTER_DUMP=true
    else
        echo "Error: Unknown option '$4'. Expected '--drop'." >&2
        exit 1
    fi
fi

# ---------------------------------------------------------------------------
# Step 2: Check that required MongoDB environment variables are set,
#         and define a helper function for running mongosh commands.
# ---------------------------------------------------------------------------
for var_name in MONGO_HOST MONGO_PORT MONGO_USERNAME MONGO_PASSWORD MONGO_DBNAME; do
    if [ -z "${!var_name:-}" ]; then
        echo "Error: Environment variable '$var_name' is not set or is empty." >&2
        exit 1
    fi
done

# Strip an optional "mongodb://" prefix because `--host` expects a hostname or IP.
CLEAN_HOST="${MONGO_HOST#mongodb://}"

# Helper to run a JavaScript snippet in mongosh against `$MONGO_DBNAME`.
run_mongosh() {
    local mongosh_js_code="$1"
    mongosh \
        --host "$CLEAN_HOST" \
        --port "$MONGO_PORT" \
        --username "$MONGO_USERNAME" \
        --password "$MONGO_PASSWORD" \
        --authenticationDatabase admin \
        --quiet \
        "$MONGO_DBNAME" \
        --eval "$mongosh_js_code"
}

# ---------------------------------------------------------------------------
# Step 3: Find `_runtime.analytics.<YYYY>_<MM>` collections older than cutoff
# ---------------------------------------------------------------------------
# Note: Conveniently, sorting `YYYY_MM` strings alphanumerically also sorts
#       them chronologically, so we can use `match[1] < cutoff` to select
#       earlier months. Example: "2024_09" < "2024_10"
echo "Finding _runtime.analytics.<YYYY>_<MM> collections older than $YYYY_MM in '$MONGO_DBNAME'..."

COLL_NAMES=$(run_mongosh "
  const cutoff = '$YYYY_MM';
  const pattern = /^_runtime\.analytics\.(\d{4}_\d{2})$/;
  db.getCollectionNames()
    .filter((name) => {
      const match = name.match(pattern);
      return match !== null && match[1] < cutoff;
    })
    .sort()
    .forEach((name) => print(name));
")

if [ -z "$COLL_NAMES" ]; then
    echo "Found 0 _runtime.analytics.<YYYY>_<MM> collections older than $YYYY_MM. Exiting."
    exit 0
fi

# ---------------------------------------------------------------------------
# Step 4: Dump each matching collection (and drop it if `--drop` was passed)
# ---------------------------------------------------------------------------
mkdir -p "$OUTPUT_DIR"

echo "Collections to archive:"
echo "$COLL_NAMES"
echo "Archive directory: $OUTPUT_DIR"

for COLL_NAME in $COLL_NAMES; do
    echo "Dumping $COLL_NAME..."
    mongodump \
        --host "$CLEAN_HOST" \
        --port "$MONGO_PORT" \
        --username "$MONGO_USERNAME" \
        --password "$MONGO_PASSWORD" \
        --authenticationDatabase admin \
        --db "$MONGO_DBNAME" \
        --collection "$COLL_NAME" \
        --gzip \
        --out "$OUTPUT_DIR"

    # Because `set -e` is enabled above, this drop only runs if `mongodump` succeeded.
    if [ "$DROP_AFTER_DUMP" = true ]; then
        echo "Dropping $COLL_NAME..."
        run_mongosh "db.getCollection('$COLL_NAME').drop()"
    fi
done

echo "Done. Archives written to: $OUTPUT_DIR"
