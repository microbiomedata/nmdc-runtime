import pytest
import requests_mock

from dagster import build_op_context

from nmdc_runtime.site.resources import nmdc_portal_api_client_resource
from nmdc_runtime.site.ops import fetch_nmdc_portal_submission_by_id


MOCK_BASE_URL = "http://example.com/nmdc_portal"
MOCK_SUBMISSION_ID = "353d751f-cff0-4558-9051-25a87ba00d3f"


@pytest.fixture
def client_config():
    return {"base_url": MOCK_BASE_URL, "refresh_token": "12345"}


@pytest.fixture
def op_context(client_config):
    return build_op_context(
        resources={
            "nmdc_portal_api_client": nmdc_portal_api_client_resource.configured(
                client_config
            )
        },
    )


def test_metadata_submission(op_context):
    with requests_mock.mock() as mock:
        mock.post(
            f"{MOCK_BASE_URL}/auth/refresh",
            json={
                "access_token": "abcde",
                "expires_in": 86400,
            },
        )
        mock.get(
            f"{MOCK_BASE_URL}/api/metadata_submission/{MOCK_SUBMISSION_ID}",
            json={"id": MOCK_SUBMISSION_ID},
        )

        # The first request should initiate an access token refresh and then fetch the submission
        fetch_nmdc_portal_submission_by_id(op_context, MOCK_SUBMISSION_ID)
        assert len(mock.request_history) == 2
        assert mock.request_history[0].url == f"{MOCK_BASE_URL}/auth/refresh"
        assert (
            mock.request_history[1].url
            == f"{MOCK_BASE_URL}/api/metadata_submission/{MOCK_SUBMISSION_ID}"
        )

        # The second request should not need to refresh the access token
        fetch_nmdc_portal_submission_by_id(
            op_context, "353d751f-cff0-4558-9051-25a87ba00d3f"
        )
        assert len(mock.request_history) == 3
        assert (
            mock.request_history[2].url
            == f"{MOCK_BASE_URL}/api/metadata_submission/{MOCK_SUBMISSION_ID}"
        )
