/**
 * This mongosh script restores all standard NMDC user-defined Mongo roles
 * (except for the "nmdc_migrator" role) to their standard states.
 * 
 * Note: This script contains excerpts from the authoritative user-defined role reference script at:
 *       https://github.com/microbiomedata/infra-admin/blob/main/mongodb/roles/createRoles.mongo.js
 *       You can compare this file to that one, using: https://www.diffchecker.com/text-compare/
 * 
 * Note: I select the database via `db.getSiblingDB()` since the `use` helper isn't available here.
 *       Reference: https://www.mongodb.com/docs/manual/reference/method/db.getSiblingDB/
 */

const db = db.getSiblingDB("admin");

db.updateRole("nmdc_runtime", {
    privileges: [],
    roles: [
        { db: "admin", role: "readWriteAnyDatabase" },
        { db: "admin", role: "dbAdminAnyDatabase" },
    ],
});

db.updateRole("nmdc_scheduler", {
    privileges: [
        { resource: { db: "nmdc", collection: "jobs" }, actions: ["find", "insert", "update", "remove"] }
    ],
    roles: [
        { db: "nmdc", role: "read" },
    ],
});

db.updateRole("nmdc_aggregator", {
    privileges: [
        { resource: { db: "nmdc", collection: "metap_gene_function_aggregation" }, actions: ["find", "insert", "update", "remove"] },
        { resource: { db: "nmdc", collection: "functional_annotation_agg" }, actions: ["find", "insert", "update", "remove"] },
    ],
    roles: [
        { db: "nmdc", role: "read" },
    ],
});

db.updateRole("nmdc_reader", {
    privileges: [
        { resource: { db: "", collection: "" }, actions: ["changeOwnPassword"] },
    ],
    roles: [
        { db: "nmdc", role: "read" },
        { db: "nmdc_updated", role: "read" },
        { db: "nmdc_deleted", role: "read" },
        { db: "nmdc_changesheet_submission_results", role: "read" },
    ],
});

db.updateRole("nmdc_editor", {
    privileges: [
        { resource: { db: "", collection: "" }, actions: ["changeOwnPassword"] },
    ],
    roles: [
        { db: "nmdc", role: "readWrite" },
        { db: "nmdc_updated", role: "readWrite" },
        { db: "nmdc_deleted", role: "readWrite" },
        { db: "nmdc_changesheet_submission_results", role: "readWrite" },
    ],
});

db.updateRole("all_dumper", {
    privileges: [
        { resource: { db: "config", collection: "system.preimages" }, actions: ["find"] },
    ],
    roles: [
        { db: "admin", role: "backup" },
    ],
});

print("✅ Access restored.");