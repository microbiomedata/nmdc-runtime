"""
Tests for the site client secret update endpoint.
"""

import pytest
import requests
from starlette import status

from nmdc_runtime.api.core.auth import get_password_hash, verify_password
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.allowance import AllowanceAction
from nmdc_runtime.api.models.site import SiteInDB, SiteClientInDB


@pytest.fixture(autouse=True)
def setup_test_site_client_allowance(api_user_client):
    """Setup test environment with site client management allowance."""
    mdb = get_mongo_db()
    
    # Grant the user permission to manage site clients
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    allowances_collection.insert_one({
        "username": api_user_client.username,
        "action": AllowanceAction.MANAGE_SITE_CLIENTS.value,
    })
    
    yield
    
    # Cleanup: Remove the test allowance
    allowances_collection.delete_one({
        "username": api_user_client.username,
        "action": AllowanceAction.MANAGE_SITE_CLIENTS.value,
    })


def test_update_site_client_secret_success(api_user_client):
    """Test successfully updating a site client's secret."""
    mdb = get_mongo_db()
    
    # Create a test site with a client
    test_site_id = "test-site-for-secret-update"
    test_client_id = "test-client-123"
    original_secret = "OriginalSecret123!"
    
    # Insert the test site
    mdb.sites.insert_one(
        SiteInDB(
            id=test_site_id,
            clients=[
                SiteClientInDB(
                    id=test_client_id,
                    hashed_secret=get_password_hash(original_secret),
                )
            ],
        ).model_dump()
    )
    
    try:
        # Update the secret
        new_secret = "NewSecret456!"
        rv = api_user_client.request(
            "PATCH",
            f"/admin/site_clients/{test_client_id}",
            {"secret": new_secret},
        )
        
        assert rv.status_code == status.HTTP_200_OK
        response_data = rv.json()
        assert "message" in response_data
        assert test_client_id in response_data["message"]
        
        # Verify the secret was actually updated in the database
        updated_site = mdb.sites.find_one({"id": test_site_id})
        assert updated_site is not None
        updated_client = next(
            (c for c in updated_site["clients"] if c["id"] == test_client_id),
            None
        )
        assert updated_client is not None
        
        # Verify the new hashed secret matches the new secret
        assert verify_password(new_secret, updated_client["hashed_secret"])
        # Verify the old secret no longer works
        assert not verify_password(original_secret, updated_client["hashed_secret"])
        
    finally:
        # Cleanup: Remove the test site
        mdb.sites.delete_one({"id": test_site_id})


def test_update_site_client_secret_not_found(api_user_client):
    """Test updating a non-existent site client returns 404."""
    non_existent_client_id = "non-existent-client-999"
    
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        api_user_client.request(
            "PATCH",
            f"/admin/site_clients/{non_existent_client_id}",
            {"secret": "SomeSecret123!"},
        )
    
    assert exc_info.value.response.status_code == status.HTTP_404_NOT_FOUND
    assert non_existent_client_id in exc_info.value.response.json()["detail"]


def test_update_site_client_secret_empty(api_user_client):
    """Test updating with an empty secret returns 400."""
    mdb = get_mongo_db()
    
    # Create a test site with a client
    test_site_id = "test-site-empty-secret"
    test_client_id = "test-client-empty"
    
    mdb.sites.insert_one(
        SiteInDB(
            id=test_site_id,
            clients=[
                SiteClientInDB(
                    id=test_client_id,
                    hashed_secret=get_password_hash("SomeSecret123!"),
                )
            ],
        ).model_dump()
    )
    
    try:
        # Try to update with an empty secret
        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            api_user_client.request(
                "PATCH",
                f"/admin/site_clients/{test_client_id}",
                {"secret": ""},
            )
        
        assert exc_info.value.response.status_code == status.HTTP_400_BAD_REQUEST
        assert "empty" in exc_info.value.response.json()["detail"].lower()
        
    finally:
        # Cleanup: Remove the test site
        mdb.sites.delete_one({"id": test_site_id})


def test_update_site_client_secret_whitespace_only(api_user_client):
    """Test updating with whitespace-only secret returns 400."""
    mdb = get_mongo_db()
    
    # Create a test site with a client
    test_site_id = "test-site-whitespace-secret"
    test_client_id = "test-client-whitespace"
    
    mdb.sites.insert_one(
        SiteInDB(
            id=test_site_id,
            clients=[
                SiteClientInDB(
                    id=test_client_id,
                    hashed_secret=get_password_hash("SomeSecret123!"),
                )
            ],
        ).model_dump()
    )
    
    try:
        # Try to update with a whitespace-only secret
        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            api_user_client.request(
                "PATCH",
                f"/admin/site_clients/{test_client_id}",
                {"secret": "   "},
            )
        
        assert exc_info.value.response.status_code == status.HTTP_400_BAD_REQUEST
        assert "empty" in exc_info.value.response.json()["detail"].lower()
        
    finally:
        # Cleanup: Remove the test site
        mdb.sites.delete_one({"id": test_site_id})


def test_update_site_client_secret_without_permission(api_user_client):
    """Test that updating without proper allowance returns 403."""
    mdb = get_mongo_db()
    
    # Remove the allowance granted by the fixture
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    allowances_collection.delete_one({
        "username": api_user_client.username,
        "action": AllowanceAction.MANAGE_SITE_CLIENTS.value,
    })
    
    # Create a test site with a client
    test_site_id = "test-site-no-permission"
    test_client_id = "test-client-no-permission"
    
    mdb.sites.insert_one(
        SiteInDB(
            id=test_site_id,
            clients=[
                SiteClientInDB(
                    id=test_client_id,
                    hashed_secret=get_password_hash("SomeSecret123!"),
                )
            ],
        ).model_dump()
    )
    
    try:
        # Try to update without permission
        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            api_user_client.request(
                "PATCH",
                f"/admin/site_clients/{test_client_id}",
                {"secret": "NewSecret123!"},
            )
        
        assert exc_info.value.response.status_code == status.HTTP_403_FORBIDDEN
        assert "allowance" in exc_info.value.response.json()["detail"].lower()
        
    finally:
        # Cleanup: Remove the test site
        mdb.sites.delete_one({"id": test_site_id})
        # Restore the allowance for other tests
        allowances_collection.insert_one({
            "username": api_user_client.username,
            "action": AllowanceAction.MANAGE_SITE_CLIENTS.value,
        })


def test_update_site_client_secret_multiple_clients(api_user_client):
    """Test updating the secret of one client doesn't affect others."""
    mdb = get_mongo_db()
    
    # Create a test site with multiple clients
    test_site_id = "test-site-multiple-clients"
    test_client_id_1 = "test-client-1"
    test_client_id_2 = "test-client-2"
    secret_1 = "Secret1!"
    secret_2 = "Secret2!"
    
    mdb.sites.insert_one(
        SiteInDB(
            id=test_site_id,
            clients=[
                SiteClientInDB(
                    id=test_client_id_1,
                    hashed_secret=get_password_hash(secret_1),
                ),
                SiteClientInDB(
                    id=test_client_id_2,
                    hashed_secret=get_password_hash(secret_2),
                ),
            ],
        ).model_dump()
    )
    
    try:
        # Update only the first client's secret
        new_secret_1 = "NewSecret1!"
        rv = api_user_client.request(
            "PATCH",
            f"/admin/site_clients/{test_client_id_1}",
            {"secret": new_secret_1},
        )
        
        assert rv.status_code == status.HTTP_200_OK
        
        # Verify the first client's secret was updated
        updated_site = mdb.sites.find_one({"id": test_site_id})
        client_1 = next(
            (c for c in updated_site["clients"] if c["id"] == test_client_id_1),
            None
        )
        assert verify_password(new_secret_1, client_1["hashed_secret"])
        
        # Verify the second client's secret was NOT changed
        client_2 = next(
            (c for c in updated_site["clients"] if c["id"] == test_client_id_2),
            None
        )
        assert verify_password(secret_2, client_2["hashed_secret"])
        
    finally:
        # Cleanup: Remove the test site
        mdb.sites.delete_one({"id": test_site_id})
