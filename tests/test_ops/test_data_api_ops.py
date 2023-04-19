import pytest
import requests_mock

from dagster import build_op_context

from nmdc_runtime.site.resources import data_api_client_resource
from nmdc_runtime.site.ops import metadata_submission


@pytest.fixture
def client_config():
    return {}


@pytest.fixture
def op_context(client_config):
    return build_op_context(
        resources={
            "data_api_client": data_api_client_resource.configured(client_config)
        },
        op_config={},
    )


def test_metadata_submission(op_context):
    with requests_mock.mock() as mock:
        mock.get(
            'https://gist.githubusercontent.com/pkalita-lbl/479005543e8d984ee7f6ddb375290f76/raw/23f184921520ef8b922356e25ec4f5487e94ec78/353d751f-cff0-4558-9051-25a87ba00d3f.json',
            json={"id": "353d751f-cff0-4558-9051-25a87ba00d3f"},
        )

        metadata_submission(op_context)

        assert len(mock.request_history) == 1
