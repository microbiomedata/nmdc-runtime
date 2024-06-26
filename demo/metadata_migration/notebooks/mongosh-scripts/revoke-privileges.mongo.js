/**
 * This mongosh script revokes all privileges from all standard NMDC user-defined Mongo roles
 * (except for the "nmdc_migrator" role).
 * 
 * Note: I select the database via `db.getSiblingDB()` since the `use` helper isn't available here.
 *       Reference: https://www.mongodb.com/docs/manual/reference/method/db.getSiblingDB/
 */


const db = db.getSiblingDB("admin");

db.updateRole("nmdc_reader",     { privileges: [], roles: [] });
db.updateRole("nmdc_editor",     { privileges: [], roles: [] });
db.updateRole("nmdc_runtime",    { privileges: [], roles: [] });
db.updateRole("nmdc_aggregator", { privileges: [], roles: [] });
db.updateRole("nmdc_scheduler",  { privileges: [], roles: [] });
db.updateRole("all_dumper",      { privileges: [], roles: [] });

print("✋ Access revoked.");