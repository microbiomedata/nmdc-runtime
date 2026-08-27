"""
Live-Mongo tests for the reload_ontology_by_prefix graph (nmdc-runtime issue 1565's scoped
drop-then-load reload strategy): delete_ontology_terms_by_prefix, then load_ontology.

Uses envo (small, fast) in place of ncbitaxon to exercise the real graph mechanics -- the
Nothing-dependency ordering between the two ops, and both ops' configs nested under one job's
run_config -- without a ~90-minute NCBITaxon-scale load. The prefix-scoping safety property
(deleting one ontology's docs must not touch a sibling ontology's docs) is what's actually novel
and risk-bearing here, and is exercised directly against real data, real indices, real Mongo.

Does not assume the shared ontology_class_set/ontology_relation_set collections start empty:
other live-DB tests in this suite (e.g. test_load_ontology_integration) load envo into the same
collections and don't clean up after themselves. This test explicitly clears its own prefixes
before asserting anything, rather than depending on suite run order.
"""

import os

import pytest
from dagster import DagsterInstance
from pymongo import MongoClient

from nmdc_runtime.site.graphs import reload_ontology_by_prefix
from nmdc_runtime.site.resources import mongo_resource

pytestmark = pytest.mark.skipif(
    os.getenv("MONGO_PASSWORD") is None or os.getenv("ENABLE_DB_TESTS") != "true",
    reason="Skipping test: Requires MONGO_PASSWORD and ENABLE_DB_TESTS=true",
)


def _client_config():
    return {
        "dbname": os.getenv("MONGO_DBNAME"),
        "host": os.getenv("MONGO_HOST"),
        "password": os.getenv("MONGO_PASSWORD"),
        "username": os.getenv("MONGO_USERNAME"),
    }


def _raw_mongo_client():
    return MongoClient(
        host=os.getenv("MONGO_HOST"),
        username=os.getenv("MONGO_USERNAME"),
        password=os.getenv("MONGO_PASSWORD"),
        directConnection=True,  # this Mongo runs as a single-node replica set in CI/local Docker
    )


def _run_config(delete_id_prefix, load_source_ontology):
    return {
        "ops": {
            "delete_ontology_terms_by_prefix": {
                "config": {"id_prefix": delete_id_prefix}
            },
            "load_ontology": {
                "config": {
                    "source_ontology": load_source_ontology,
                    "mode": "fast-initial",
                    "closure": "isa",
                }
            },
        }
    }


def _clear_prefix(db, id_prefix):
    db["ontology_class_set"].delete_many({"id": {"$regex": f"^{id_prefix}"}})
    db["ontology_relation_set"].delete_many({"subject": {"$regex": f"^{id_prefix}"}})


def test_reload_deletes_by_prefix_without_touching_a_sibling_ontology():
    """
    The prefix-scoping safety property this reload strategy depends on: deleting one ontology's
    docs by id/subject prefix must leave a differently-prefixed sibling ontology's docs untouched.
    Seeds fake sibling-ontology docs directly (cheap, deterministic), and real envo docs via a
    real fast-initial load, then confirms the delete step only removes the envo-prefixed ones.
    """
    job = reload_ontology_by_prefix.to_job(resource_defs={"mongo": mongo_resource})
    resources = {"mongo": mongo_resource.configured(_client_config())}
    client = _raw_mongo_client()
    db = client[os.getenv("MONGO_DBNAME")]

    # This suite's other live-DB tests share these collections and don't clean up after
    # themselves, so start from a known state for the two prefixes this test owns.
    _clear_prefix(db, "ENVO:")
    _clear_prefix(db, "SIBLING_ONTOLOGY:")

    # Everything from here on writes to the shared live collections, so it's wrapped in try/
    # finally: a failure partway through (a job run, an insert, an assertion) must still leave
    # these two prefixes cleared, or a later test run fails on stale data or duplicate IDs.
    try:
        # Each execute_in_process() call gets its own explicit ephemeral instance. Relying on the
        # implicit default across two calls in one test process hit a Dagster SQLite event-log
        # teardown/reinit issue on the second call ("no such table: event_logs") unrelated to this
        # graph's own logic.
        first_load = job.execute_in_process(
            resources=resources,
            # id_prefix "ENVO:" here is a harmless no-op delete: the collections were just cleared.
            run_config=_run_config(
                delete_id_prefix="ENVO:", load_source_ontology="envo"
            ),
            instance=DagsterInstance.ephemeral(),
        )
        assert first_load.success

        db["ontology_class_set"].insert_one(
            {
                "id": "SIBLING_ONTOLOGY:0001",
                "name": "fake sibling class",
                "type": "nmdc:OntologyClass",
            }
        )
        db["ontology_relation_set"].insert_one(
            {
                "subject": "SIBLING_ONTOLOGY:0001",
                "predicate": "rdfs:subClassOf",
                "object": "SIBLING_ONTOLOGY:0000",
                "type": "nmdc:OntologyRelation",
            }
        )
        envo_class_count_before = db["ontology_class_set"].count_documents(
            {"id": {"$regex": "^ENVO:"}}
        )
        assert (
            envo_class_count_before > 0
        ), "envo should already be loaded from the first job run"

        # Now reload envo again: delete its docs, then load them back. The sibling docs must survive.
        reload_result = job.execute_in_process(
            resources=resources,
            run_config=_run_config(
                delete_id_prefix="ENVO:", load_source_ontology="envo"
            ),
            instance=DagsterInstance.ephemeral(),
        )
        assert reload_result.success

        sibling_class = db["ontology_class_set"].find_one(
            {"id": "SIBLING_ONTOLOGY:0001"}
        )
        sibling_relation = db["ontology_relation_set"].find_one(
            {"subject": "SIBLING_ONTOLOGY:0001"}
        )
        assert (
            sibling_class is not None
        ), "reloading envo must not delete a sibling ontology's classes"
        assert (
            sibling_relation is not None
        ), "reloading envo must not delete a sibling ontology's relations"

        envo_class_count_after = db["ontology_class_set"].count_documents(
            {"id": {"$regex": "^ENVO:"}}
        )
        assert (
            envo_class_count_after == envo_class_count_before
        ), "envo should have been fully deleted and fully reloaded, ending at the same count"

        delete_op_result = reload_result.output_for_node(
            "delete_ontology_terms_by_prefix"
        )
        assert delete_op_result["class_deleted_count"] == envo_class_count_before
    finally:
        _clear_prefix(db, "ENVO:")
        _clear_prefix(db, "SIBLING_ONTOLOGY:")
