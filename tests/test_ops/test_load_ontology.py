import os
import pytest
from dagster import build_op_context
from nmdc_runtime.site.resources import mongo_resource
from nmdc_runtime.site.ops import (
    load_ontology
)


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

def test_load_ontology(op_context):
    counter = load_ontology(op_context)
    assert counter == 0