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
    assert sum(
        mdb[collection_name].estimated_document_count()
        for collection_name in collection_names
    ) == materialize_alldocs(op_context)
