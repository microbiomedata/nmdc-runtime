import os

import pytest
from dagster import build_op_context

from nmdc_runtime.api.endpoints.metadata import _ensure_job__metadata_in
from nmdc_runtime.site.repository import preset_normal
from nmdc_runtime.site.resources import (
    mongo_resource,
    runtime_api_site_client_resource,
    runtime_api_user_client_resource,
)

from nmdc_runtime.site.graphs import apply_metadata_in
from tests.lib.faker import Faker


@pytest.fixture
def op_context():
    return build_op_context(
        resources={
            "mongo": mongo_resource.configured(
                {
                    "dbname": os.getenv("MONGO_DBNAME"),
                    "host": os.getenv("MONGO_HOST"),
                    "password": os.getenv("MONGO_PASSWORD"),
                    "username": os.getenv("MONGO_USERNAME"),
                }
            ),
            "runtime_api_user_client": runtime_api_user_client_resource.configured(
                {
                    "base_url": os.getenv("API_HOST"),
                    "username": os.getenv("API_ADMIN_USER"),
                    "password": os.getenv("API_ADMIN_PASS"),
                },
            ),
            "runtime_api_site_client": runtime_api_site_client_resource.configured(
                {
                    "base_url": os.getenv("API_HOST"),
                    "site_id": os.getenv("API_SITE_ID"),
                    "client_id": os.getenv("API_SITE_CLIENT_ID"),
                    "client_secret": os.getenv("API_SITE_CLIENT_SECRET"),
                }
            ),
        }
    )


def test_apply_metadata_in_functional_annotation_agg(op_context):
    mongo = op_context.resources.mongo
    db = mongo.db  # concise alias
    
    # Insert the _referenced_ documents first, so that referential integrity is maintained.
    faker = Faker()
    workflow_execution: dict = faker.generate_metagenome_annotations(
        1,
        was_informed_by=['nmdc:dgns-00-000001'],
        has_input=['nmdc:bsm-00-000001']
    )[0]
    filter_ = {"id": workflow_execution["id"]}
    assert db.workflow_execution.count_documents(filter_) == 0
    db.workflow_execution.insert_one(workflow_execution)

    docs = {
        "functional_annotation_agg": [
            {
                "was_generated_by": "nmdc:wfmtan-13-hemh0a82.1",
                "gene_function_id": "KEGG.ORTHOLOGY:K00005",
                "count": 10,
                "type": "nmdc:FunctionalAnnotationAggMember",
            },
            {
                "was_generated_by": "nmdc:wfmtan-13-hemh0a82.1",
                "gene_function_id": "KEGG.ORTHOLOGY:K01426",
                "count": 5,
                "type": "nmdc:FunctionalAnnotationAggMember",
            },
        ]
    }
    # Ensure the docs are not already in the test database.
    for doc_spec in docs["functional_annotation_agg"]:
        db.functional_annotation_agg.delete_many(doc_spec)

    extra_run_config_data = _ensure_job__metadata_in(
        docs,
        op_context.resources.runtime_api_user_client.username,
        db,
        op_context.resources.runtime_api_site_client.client_id,
        drs_object_exists_ok=True,  # If there exists a DRS object with a matching checksum, use it.
    )

    apply_metadata_in.to_job(**preset_normal).execute_in_process(
        run_config=extra_run_config_data
    )

    assert (
        db.functional_annotation_agg.count_documents(
            {"$or": docs["functional_annotation_agg"]}
        )
        == 2
    )

    # Clean up the test database.
    db.workflow_execution.delete_many(filter_)
    for doc_spec in docs["functional_annotation_agg"]:
        db.functional_annotation_agg.delete_many(doc_spec)
