import os
import pytest
import requests

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.minter.config import schema_classes
from nmdc_runtime.site.resources import RuntimeApiSiteClient
from tests.test_api.test_endpoints import ensure_test_resources

schema_class = schema_classes()[0]["id"]


@pytest.fixture
def mint_one_id_response(api_site_client: RuntimeApiSiteClient):
    rv = api_site_client.mint_id(schema_class).json()

    yield rv

    # Delete minted ID if the fixture-using test did not delete it already.
    try:
        rv = api_site_client.request("GET", f"/pids/resolve/{rv[0]}")
    except requests.HTTPError as exc_info:
        if exc_info.response.status_code == 404:
            return

        assert (
            api_site_client.request(
                "POST",
                f"/pids/delete",
                {"id_name": rv[0]},
            ).status_code
            == 200
        )


def test_minter_api_mint(mint_one_id_response):
    rv = mint_one_id_response
    assert len(rv) == 1 and rv[0].startswith("nmdc:")


def test_minter_api_resolve(mint_one_id_response, api_site_client):
    id_name = mint_one_id_response[0]
    rv = api_site_client.request("GET", f"/pids/resolve/{id_name}").json()
    assert rv["id"] == id_name and rv["status"] == "draft"


def test_minter_api_bind(mint_one_id_response, api_site_client):
    id_name = mint_one_id_response[0]
    rv = api_site_client.request(
        "POST",
        f"/pids/bind",
        {"id_name": id_name, "metadata_record": {"foo": "bar"}},
    ).json()
    assert (
        rv["id"] == id_name
        and rv["status"] == "draft"
        and rv["bindings"] == {"foo": "bar"}
    )


def test_minter_api_delete(mint_one_id_response, api_site_client):
    id_name = mint_one_id_response[0]
    rv = api_site_client.request(
        "POST",
        f"/pids/delete",
        {"id_name": id_name},
    )
    assert rv.status_code == 200
