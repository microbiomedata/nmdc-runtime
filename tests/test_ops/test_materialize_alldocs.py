import os

import pytest

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
    collection_names = populated_schema_collection_names_with_id_field(mdb)

    # Invoke the function-under-test.
    #
    # Note: It returns an estimated count; so, we'll just verify that it's an integer,
    #       rather than relying on its value. We'll get an _exact_ count later.
    #
    estimated_number_of_docs_in_alldocs = materialize_alldocs(op_context)
    assert isinstance(estimated_number_of_docs_in_alldocs, int)

    # Get a reference to the newly-materialized `alldocs` collection.
    alldocs = mdb.get_collection("alldocs")
    num_alldocs_docs = alldocs.count_documents({})  # here, we get an _exact_ count

    # Verify each upstream document's `id` value appears in exactly one document in the `alldocs` collection.
    #
    # Note: While we iterate through the upstream collections, we also count the number
    #       of documents in those collections. We'll use that number in a future assertion.
    #
    num_upstream_docs = 0
    for collection_name in collection_names:
        collection = mdb.get_collection(collection_name)
        for document in collection.find({}):
            num_upstream_docs += 1
            document_id = document["id"]
            assert alldocs.count_documents({"id": document_id}) == 1

    # Verify the value in the `type` field of each document in the `alldocs` collection is of type "array".
    # Reference: https://www.mongodb.com/docs/manual/reference/operator/query/type/#arrays
    assert alldocs.count_documents({"type": {"$type": "array"}}) == num_alldocs_docs

    # Verify the total number of documents in all the upstream collections, combined,
    # equals the number of documents in the `alldocs` collection.
    assert num_upstream_docs == num_alldocs_docs
