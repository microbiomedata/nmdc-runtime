import os

import pytest
from toolz import assoc, dissoc

from dagster import build_op_context

from nmdc_runtime.site.resources import mongo_resource
from nmdc_runtime.site.ops import (
    materialize_alldocs,
)
from nmdc_runtime.util import populated_schema_collection_names_with_id_field


@pytest.fixture
def client_config():
    return {
        "dbname": os.getenv("MONGO_DBNAME"),
        "host": os.getenv("MONGO_HOST"),
        "password": os.getenv("MONGO_PASSWORD"),
        "username": os.getenv("MONGO_USERNAME"),
    }


@pytest.fixture
def op_context(client_config):
    return build_op_context(
        resources={"mongo": mongo_resource.configured(client_config)}
    )


def test_materialize_alldocs(op_context):
    mdb = op_context.resources.mongo.db

    # Insert some documents into some upstream collections.
    #
    # Note: This will allow us to look for _specific_ documents in the resulting `alldocs` collection.
    #
    # Note: This collection was chosen mostly arbitrarily. I chose it because I saw that other tests were
    #       not (currently) leaving "residual documents" in it (note: at the time of this writing, the
    #       test database is _not_ being rolled back to a pristine state in between tests).
    #
    # Reference: https://microbiomedata.github.io/berkeley-schema-fy24/FieldResearchSite/#direct
    #
    field_research_site_class_ancestry_chain = ["FieldResearchSite", "Site", "MaterialEntity", "NamedThing"]
    field_research_site_documents = [
        {"id": "frsite-99-00000001", "type": "nmdc:FieldResearchSite", "name": "Site A"},
        {"id": "frsite-99-00000002", "type": "nmdc:FieldResearchSite", "name": "Site B"},
        {"id": "frsite-99-00000003", "type": "nmdc:FieldResearchSite", "name": "Site C"},
    ]
    field_research_site_set_collection = mdb.get_collection("field_research_site_set")
    for document in field_research_site_documents:
        field_research_site_set_collection.replace_one(document, document, upsert=True)

    # Get a list of non-empty collections in which at least one document has an `id` field.
    #
    # Note: That is the same criteria the function-under-test uses to identify which upstream collections
    #       it will source (i.e. copy) documents from in order to populate the `alldocs` collection.
    #
    collection_names = populated_schema_collection_names_with_id_field(mdb)
    assert "field_research_site_set" in collection_names

    # Invoke the function-under-test.
    #
    # Note: It returns an estimated count; so, we'll just verify that it's an integer,
    #       rather than relying on its value. We'll get an _exact_ count later.
    #
    estimated_number_of_docs_in_alldocs = materialize_alldocs(op_context)
    assert isinstance(estimated_number_of_docs_in_alldocs, int)

    # Get a reference to the newly-materialized `alldocs` collection.
    alldocs_collection = mdb.get_collection("alldocs")
    num_alldocs_docs = alldocs_collection.count_documents({})  # here, we get an _exact_ count

    # Verify each upstream document is represented correctly—and only once—in the `alldocs` collection.
    #
    # Note: We do not check the `type` value here (beyond its data type), due to the current tedium of determining
    #       the class ancestry chain from a dictionary (as opposed to a Python instance). We do check it for some
    #       documents later, but only for documents we inserted above, since we know what to "expect" for those
    #       documents. Here, we just verify that each document's `type` value is of type `array`.
    #
    # Note: We also keep a tally of the number of upstream documents that exist, which we'll reference later.
    #
    num_upstream_docs = 0
    for collection_name in collection_names:
        collection = mdb.get_collection(collection_name)
        for document in collection.find({}):
            num_upstream_docs += 1
            document_lacking_type = dissoc(document, "_id", "type")
            document_having_generic_type = assoc(document_lacking_type, "type", {"$type": "array"})
            assert alldocs_collection.count_documents(document_having_generic_type) == 1

    # Verify each of the specific documents we created above appears in the `alldocs` collection once,
    # and that its `type` value has been replaced with its class ancestry chain.
    for document in field_research_site_documents:
        alldocs_document = assoc(dissoc(document, "type"), "type", field_research_site_class_ancestry_chain)
        assert alldocs_collection.count_documents(alldocs_document) == 1

    # Verify the total number of documents in all the upstream collections, combined,
    # equals the number of documents in the `alldocs` collection.
    assert num_upstream_docs == num_alldocs_docs

    # Clean up: Delete the documents we created within this test, from the database.
    for document in field_research_site_documents:
        field_research_site_set_collection.delete_one(document)
    alldocs_collection.delete_many({})
