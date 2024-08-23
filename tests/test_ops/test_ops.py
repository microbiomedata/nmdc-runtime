import json
import os

import pytest
from dagster import build_op_context, RunConfig

from nmdc_runtime.api.endpoints.metadata import _ensure_job__metadata_in
from nmdc_runtime.api.endpoints.util import persist_content_and_get_drs_object
from nmdc_runtime.site.repository import preset_normal
from nmdc_runtime.site.resources import (
    mongo_resource,
    runtime_api_site_client_resource,
    RuntimeApiSiteClient,
    runtime_api_user_client_resource,
    RuntimeApiUserClient,
)

from nmdc_runtime.site.graphs import apply_metadata_in


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
    docs = {
        "functional_annotation_agg": [
            {
                "metagenome_annotation_id": "nmdc:wfmtan-13-hemh0a82.1",
                "gene_function_id": "KEGG.ORTHOLOGY:K00005",
                "count": 10,
            },
            {
                "metagenome_annotation_id": "nmdc:wfmtan-13-hemh0a82.1",
                "gene_function_id": "KEGG.ORTHOLOGY:K01426",
                "count": 5,
            },
        ]
    }
    # Ensure the docs are not already in the test database.
    for doc_spec in docs["functional_annotation_agg"]:
        mongo.db.functional_annotation_agg.delete_many(doc_spec)

    extra_run_config_data = _ensure_job__metadata_in(
        docs,
        op_context.resources.runtime_api_user_client.username,
        mongo.db,
        op_context.resources.runtime_api_site_client.client_id,
        drs_object_exists_ok=True,  # If there exists a DRS object with a matching checksum, use it.
    )

    apply_metadata_in.to_job(**preset_normal).execute_in_process(
        run_config=extra_run_config_data
    )

    assert (
        mongo.db.functional_annotation_agg.count_documents(
            {"$or": docs["functional_annotation_agg"]}
        )
        == 2
    )
