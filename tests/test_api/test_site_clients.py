"""
Tests targeting the endpoint that can be used to update a site client.
"""

import pytest
import requests
from starlette import status

from nmdc_runtime.api.core.auth import get_password_hash, verify_password
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.allowance import AllowanceAction
from nmdc_runtime.api.models.site import SiteInDB, SiteClientInDB


@pytest.fixture()
def api_user_client_with_site_client_management_allowance(api_user_client):
    """Yields an API user client for a user having a specific allowance."""

    mdb = get_mongo_db()

    allowance = {
        "username": api_user_client.username,
        "action": AllowanceAction.MANAGE_SITE_CLIENTS.value,
    }

    allowances_coll = mdb.get_collection("_runtime.api.allow")
    
    # Grant the allowance.
    allowances_coll.insert_one(allowance)
    
    yield api_user_client
    
    # Cleanup: Revoke the allowance.
    allowances_coll.delete_one(allowance)


@pytest.fixture()
def db_containing_site_client():
    """Yields a database containing a site with a site client."""
    
    mdb = get_mongo_db()

    # Create a test site having a site client having a known secret.
    site_id = "__test_site_id"
    site_client_id = "__test_site_client_id"
    initial_secret = "__test_client_initial_secret"

    mdb.sites.insert_one(
        SiteInDB(
            id=site_id,
            clients=[
                SiteClientInDB(
                    id=site_client_id,
                    hashed_secret=get_password_hash(initial_secret),
                )
            ],
        ).model_dump()
    )

    yield mdb

    # Cleanup: Remove the test site.
    mdb.sites.delete_one({"id": site_id})


def test_update_site_client_secret(
    api_user_client_with_site_client_management_allowance,
    db_containing_site_client,
):
    api_user_client = api_user_client_with_site_client_management_allowance  # concise alias
    mdb = db_containing_site_client  # concise alias

    site_client_id = "__test_site_client_id"

    # Test: When we omit the "secret" from the payload, we get an HTTP 204.
    response = api_user_client.request(
        "PATCH",
        f"/admin/site_clients/{site_client_id}",
        {},
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT

    # Test: When the new secret is too short, we get an HTTP 400.
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        api_user_client.request(
            "PATCH",
            f"/admin/site_clients/{site_client_id}",
            {"secret": "2short"},
        )
    assert exc_info.value.response.status_code == status.HTTP_400_BAD_REQUEST

    # Test: Update the secret successfully.
    new_secret = "my_new_secret!"
    response = api_user_client.request(
        "PATCH",
        f"/admin/site_clients/{site_client_id}",
        {"secret": new_secret},
    )
    assert response.status_code == status.HTTP_200_OK
    updated_site_client = mdb.sites.find_one(
        {"clients.id": site_client_id},
        {"clients.$": 1},
    )["clients"][0]
    assert updated_site_client["id"] == site_client_id
    assert verify_password(new_secret, updated_site_client["hashed_secret"])
