import os

import pytest
import requests
from starlette import status

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.site.resources import get_mongo, RuntimeApiUserClient
from nmdc_runtime.site.repository import run_config_frozen__normal_env
from tests.test_api.test_endpoints import ensure_test_resources


@pytest.fixture(scope="module")
def test_setup():
    """Setup test resources and return necessary data."""
    mdb = get_mongo(run_config_frozen__normal_env).db
    rs = ensure_test_resources(mdb)
    base_url = os.getenv("API_HOST")

    # Get authentication token
    rv = requests.post(
        base_url + "/token",
        data={
            "grant_type": "password",
            "username": rs["user"]["username"],
            "password": rs["user"]["password"],
        },
    )
    token = rv.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    return {
        "base_url": base_url,
        "headers": headers,
        "mdb": mdb,
        "username": rs["user"]["username"],
    }


@pytest.fixture(autouse=True)
def cleanup_test_allowances(test_setup):
    """Cleanup test allowances before and after each test."""
    mdb = test_setup["mdb"]
    # Clean up before test
    mdb["_runtime.api.allow"].delete_many(
        {"username": {"$in": ["test_user_1", "test_user_2", "test_user_3"]}}
    )
    mdb["_runtime.api.allow"].delete_many(
        {"action": {"$in": ["/test/action1", "/test/action2", "/test/action3"]}}
    )

    yield

    # Clean up after test
    mdb["_runtime.api.allow"].delete_many(
        {"username": {"$in": ["test_user_1", "test_user_2", "test_user_3"]}}
    )
    mdb["_runtime.api.allow"].delete_many(
        {"action": {"$in": ["/test/action1", "/test/action2", "/test/action3"]}}
    )


def test_list_all_allowances(test_setup):
    """Test listing all allowances."""
    base_url = test_setup["base_url"]
    headers = test_setup["headers"]
    mdb = test_setup["mdb"]

    # Create some test allowances
    test_allowances = [
        {"username": "test_user_1", "action": "/test/action1"},
        {"username": "test_user_2", "action": "/test/action2"},
    ]
    for allowance in test_allowances:
        mdb["_runtime.api.allow"].insert_one(allowance)

    # Get all allowances
    rv = requests.get(
        f"{base_url}/allowances",
        headers=headers,
    )

    assert rv.status_code == status.HTTP_200_OK
    allowances = rv.json()
    assert isinstance(allowances, list)
    # Should include our test allowances plus any default ones
    assert len(allowances) >= 2


def test_list_allowances_by_username(test_setup):
    """Test listing allowances filtered by username."""
    base_url = test_setup["base_url"]
    headers = test_setup["headers"]
    mdb = test_setup["mdb"]

    # Create test allowances
    mdb["_runtime.api.allow"].insert_many(
        [
            {"username": "test_user_1", "action": "/test/action1"},
            {"username": "test_user_1", "action": "/test/action2"},
            {"username": "test_user_2", "action": "/test/action3"},
        ]
    )

    # Get allowances for test_user_1
    rv = requests.get(
        f"{base_url}/allowances?username=test_user_1",
        headers=headers,
    )

    assert rv.status_code == status.HTTP_200_OK
    allowances = rv.json()
    assert len(allowances) == 2
    assert all(a["username"] == "test_user_1" for a in allowances)


def test_list_allowances_by_action(test_setup):
    """Test listing allowances filtered by action."""
    base_url = test_setup["base_url"]
    headers = test_setup["headers"]
    mdb = test_setup["mdb"]

    # Create test allowances
    mdb["_runtime.api.allow"].insert_many(
        [
            {"username": "test_user_1", "action": "/test/action1"},
            {"username": "test_user_2", "action": "/test/action1"},
            {"username": "test_user_3", "action": "/test/action2"},
        ]
    )

    # Get allowances for /test/action1
    rv = requests.get(
        f"{base_url}/allowances?action=/test/action1",
        headers=headers,
    )

    assert rv.status_code == status.HTTP_200_OK
    allowances = rv.json()
    assert len(allowances) == 2
    assert all(a["action"] == "/test/action1" for a in allowances)


def test_list_allowance_by_username_and_action(test_setup):
    """Test listing specific allowance by both username and action."""
    base_url = test_setup["base_url"]
    headers = test_setup["headers"]
    mdb = test_setup["mdb"]

    # Create test allowance
    mdb["_runtime.api.allow"].insert_one(
        {"username": "test_user_1", "action": "/test/action1"}
    )

    # Get specific allowance
    rv = requests.get(
        f"{base_url}/allowances?username=test_user_1&action=/test/action1",
        headers=headers,
    )

    assert rv.status_code == status.HTTP_200_OK
    allowances = rv.json()
    assert len(allowances) == 1
    assert allowances[0]["username"] == "test_user_1"
    assert allowances[0]["action"] == "/test/action1"


def test_create_allowance(test_setup):
    """Test creating a new allowance."""
    base_url = test_setup["base_url"]
    headers = test_setup["headers"]
    mdb = test_setup["mdb"]

    # Create allowance
    allowance_data = {
        "username": "test_user_1",
        "action": "/test/action1",
    }
    rv = requests.post(
        f"{base_url}/allowances",
        headers=headers,
        json=allowance_data,
    )

    assert rv.status_code == status.HTTP_201_CREATED
    created = rv.json()
    assert created["username"] == "test_user_1"
    assert created["action"] == "/test/action1"

    # Verify it was created in the database
    db_allowance = mdb["_runtime.api.allow"].find_one(
        {
            "username": "test_user_1",
            "action": "/test/action1",
        }
    )
    assert db_allowance is not None


def test_create_duplicate_allowance(test_setup):
    """Test that creating a duplicate allowance returns a conflict error."""
    base_url = test_setup["base_url"]
    headers = test_setup["headers"]
    mdb = test_setup["mdb"]

    # Create initial allowance
    mdb["_runtime.api.allow"].insert_one(
        {
            "username": "test_user_1",
            "action": "/test/action1",
        }
    )

    # Try to create duplicate
    allowance_data = {
        "username": "test_user_1",
        "action": "/test/action1",
    }
    rv = requests.post(
        f"{base_url}/allowances",
        headers=headers,
        json=allowance_data,
    )

    assert rv.status_code == status.HTTP_409_CONFLICT


def test_delete_allowance(test_setup):
    """Test deleting an allowance."""
    base_url = test_setup["base_url"]
    headers = test_setup["headers"]
    mdb = test_setup["mdb"]

    # Create allowance to delete
    mdb["_runtime.api.allow"].insert_one(
        {
            "username": "test_user_1",
            "action": "/test/action1",
        }
    )

    # Delete the allowance
    rv = requests.delete(
        f"{base_url}/allowances?username=test_user_1&action=/test/action1",
        headers=headers,
    )

    assert rv.status_code == status.HTTP_204_NO_CONTENT

    # Verify it was deleted from the database
    db_allowance = mdb["_runtime.api.allow"].find_one(
        {
            "username": "test_user_1",
            "action": "/test/action1",
        }
    )
    assert db_allowance is None


def test_delete_nonexistent_allowance(test_setup):
    """Test that deleting a non-existent allowance returns a 404 error."""
    base_url = test_setup["base_url"]
    headers = test_setup["headers"]

    # Try to delete non-existent allowance
    rv = requests.delete(
        f"{base_url}/allowances?username=nonexistent&action=/nonexistent/action",
        headers=headers,
    )

    assert rv.status_code == status.HTTP_404_NOT_FOUND


def test_list_valid_actions(test_setup):
    """Test listing all valid actions."""
    base_url = test_setup["base_url"]
    headers = test_setup["headers"]
    mdb = test_setup["mdb"]

    # Create test allowances with different actions
    mdb["_runtime.api.allow"].insert_many(
        [
            {"username": "test_user_1", "action": "/test/action1"},
            {"username": "test_user_2", "action": "/test/action2"},
            {"username": "test_user_3", "action": "/test/action1"},  # Duplicate action
        ]
    )

    # Get valid actions
    rv = requests.get(
        f"{base_url}/allowances/actions",
        headers=headers,
    )

    assert rv.status_code == status.HTTP_200_OK
    actions = rv.json()
    assert isinstance(actions, list)
    # Should include our test actions plus any default ones
    assert "/test/action1" in actions
    assert "/test/action2" in actions
    # Should be sorted
    assert actions == sorted(actions)


def test_allowances_require_authentication(test_setup):
    """Test that all allowance endpoints require authentication."""
    base_url = test_setup["base_url"]

    # Test without authentication headers
    endpoints_and_methods = [
        ("GET", f"{base_url}/allowances"),
        ("POST", f"{base_url}/allowances"),
        ("DELETE", f"{base_url}/allowances?username=test&action=/test"),
        ("GET", f"{base_url}/allowances/actions"),
    ]

    for method, url in endpoints_and_methods:
        if method == "POST":
            rv = requests.post(url, json={"username": "test", "action": "/test"})
        elif method == "DELETE":
            rv = requests.delete(url)
        else:  # GET
            rv = requests.get(url)

        assert rv.status_code == status.HTTP_401_UNAUTHORIZED


def test_composite_index_uniqueness(test_setup):
    """Test that the composite index enforces uniqueness on (username, action)."""
    mdb = test_setup["mdb"]

    # Insert an allowance
    mdb["_runtime.api.allow"].insert_one(
        {
            "username": "test_user_1",
            "action": "/test/action1",
        }
    )

    # Try to insert duplicate - should raise DuplicateKeyError
    from pymongo.errors import DuplicateKeyError

    with pytest.raises(DuplicateKeyError):
        mdb["_runtime.api.allow"].insert_one(
            {
                "username": "test_user_1",
                "action": "/test/action1",
            }
        )


def test_delete_allowance_missing_parameters(test_setup):
    """Test that delete endpoint requires both username and action parameters."""
    base_url = test_setup["base_url"]
    headers = test_setup["headers"]

    # Try to delete without action parameter
    rv = requests.delete(
        f"{base_url}/allowances?username=test_user_1",
        headers=headers,
    )
    assert rv.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    # Try to delete without username parameter
    rv = requests.delete(
        f"{base_url}/allowances?action=/test/action1",
        headers=headers,
    )
    assert rv.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
