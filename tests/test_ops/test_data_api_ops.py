import pytest
import requests_mock

from dagster import build_op_context

from nmdc_runtime.site.resources import nmdc_portal_api_client_resource
from nmdc_runtime.site.ops import fetch_nmdc_portal_submission_by_id


@pytest.fixture
def client_config():
    return {"base_url": "http://example.com/nmdc_portal", "session_cookie": "12345"}


@pytest.fixture
def op_context(client_config):
    return build_op_context(
        resources={
            "nmdc_portal_api_client": nmdc_portal_api_client_resource.configured(
                client_config
            )
        },
        op_config={},
    )


def test_metadata_submission(op_context):
    with requests_mock.mock() as mock:
        mock.get(
            "http://example.com/nmdc_portal/api/metadata_submission/353d751f-cff0-4558-9051-25a87ba00d3f",
            json={"id": "353d751f-cff0-4558-9051-25a87ba00d3f"},
        )

        fetch_nmdc_portal_submission_by_id(
            op_context, "353d751f-cff0-4558-9051-25a87ba00d3f"
        )

        assert len(mock.request_history) == 1
