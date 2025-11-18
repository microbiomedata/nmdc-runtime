import pytest
import requests
from starlette import status

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.allowance import AllowanceActions


@pytest.fixture(autouse=True)
def cleanup_test_allowances(api_user_client):
    """Cleanup test allowances before and after each test."""
    mdb = get_mongo_db()
    # grant /allowances permissions to api_user_client
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    allow_spec = {
        "username": api_user_client.username,
        "action": "/allowances",
    }
    allowances_collection.replace_one(allow_spec, allow_spec, upsert=True)
    # Clean up before test
    allowances_collection.delete_many(
        {"username": {"$in": ["test_user_1", "test_user_2", "test_user_3"]}}
    )
    allowances_collection.delete_many(
        {"action": {"$in": ["/test/action1", "/test/action2", "/test/action3"]}}
    )

    yield

    # Clean up after test
    allowances_collection.delete_many(
        {"username": {"$in": ["test_user_1", "test_user_2", "test_user_3"]}}
    )
    allowances_collection.delete_many(
        {"action": {"$in": ["/test/action1", "/test/action2", "/test/action3"]}}
    )
    allowances_collection.delete_one(allow_spec)



def test_list_all_allowances(api_user_client):
    """Test listing all allowances."""
    mdb = get_mongo_db()
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    # Create some test allowances
    test_allowances = [
        {"username": "test_user_1", "action": "/test/action1"},
        {"username": "test_user_2", "action": "/test/action2"},
    ]
    for allowance in test_allowances:
        allowances_collection.insert_one(allowance)
    
    # Get all allowances
    rv = api_user_client.request(
        "GET",
        "/allowances",
    )

    assert rv.status_code == status.HTTP_200_OK
    allowances = rv.json()
    # Should include our test allowances plus any default ones
    assert len(allowances["resources"]) >= 2


def test_list_allowances_by_username(api_user_client):
    """Test listing allowances filtered by username."""
    mdb = get_mongo_db()
    allowances_collection = mdb.get_collection("_runtime.api.allow")

    # Create test allowances
    allowances_collection.insert_many(
        [
            {"username": "test_user_1", "action": "/test/action1"},
            {"username": "test_user_1", "action": "/test/action2"},
            {"username": "test_user_2", "action": "/test/action3"},
        ]
    )

    # Get allowances for test_user_1
    rv = api_user_client.request(
        "GET",
        "/allowances?username=test_user_1",
    )

    assert rv.status_code == status.HTTP_200_OK
    allowances = rv.json()
    assert len(allowances["resources"]) == 2
    assert all(a["username"] == "test_user_1" for a in allowances["resources"])


def test_list_allowances_by_action(api_user_client):
    """Test listing allowances filtered by action."""
    mdb = get_mongo_db()
    allowances_collection = mdb.get_collection("_runtime.api.allow")

    # Create test allowances
    allowances_collection.insert_many(
        [
            {"username": "test_user_1", "action": AllowanceActions.SUBMIT.value},
            {"username": "test_user_2", "action": AllowanceActions.SUBMIT.value},
            {"username": "test_user_3", "action": AllowanceActions.DELETE.value},
        ]
    )

    # Get allowances for /test/action1
    rv = api_user_client.request(
        "GET",
        f"/allowances?action={AllowanceActions.SUBMIT.value}",
    )

    assert rv.status_code == status.HTTP_200_OK
    allowances = rv.json()
    assert len(allowances["resources"]) == 3
    assert all(a["action"] == AllowanceActions.SUBMIT.value for a in allowances["resources"])



def test_list_allowance_by_username_and_action(api_user_client):
    """Test listing specific allowance by both username and action."""
    mdb = get_mongo_db()
    allowances_collection = mdb.get_collection("_runtime.api.allow")

    # Create test allowance
    allowances_collection.insert_many(
        [
        {"username": "test_user_1", "action": AllowanceActions.SUBMIT.value},
        {"username": "test_user_1", "action": AllowanceActions.DELETE.value},
    ]
    )

    # Get specific allowance
    rv = api_user_client.request(
        "GET",
        f"/allowances?username=test_user_1&action={AllowanceActions.SUBMIT.value}",
    )

    assert rv.status_code == status.HTTP_200_OK
    allowances = rv.json()
    assert len(allowances["resources"]) == 1
    assert allowances["resources"][0]["username"] == "test_user_1"
    assert allowances["resources"][0]["action"] == AllowanceActions.SUBMIT.value


def test_create_allowance(api_user_client):
    """Test creating a new allowance."""
    mdb = get_mongo_db()
    allowances_collection = mdb.get_collection("_runtime.api.allow")

    # Create allowance
    allowance_data = {
        "username": "test_user_1",
        "action": AllowanceActions.SUBMIT.value,
    }
    # assert allowance_data does not already exist
    assert ( allowances_collection.count_documents(allowance_data) == 0)
    rv = api_user_client.request(
        "POST",
        "/allowances",
        allowance_data
    )

    assert rv.status_code == status.HTTP_201_CREATED
    created = rv.json()
    assert created["username"] == "test_user_1"
    assert created["action"] == AllowanceActions.SUBMIT.value

    # Verify it was created in the database
    db_allowance = allowances_collection.find_one(
        {
            "username": "test_user_1",
            "action": AllowanceActions.SUBMIT.value,
        }
    )
    assert db_allowance is not None


def test_create_duplicate_allowance(api_user_client):
    """Test that creating a duplicate allowance returns a conflict error."""
    mdb = get_mongo_db()
    allowances_collection = mdb.get_collection("_runtime.api.allow")

    # Create initial allowance
    allowances_collection.insert_one(
        {
            "username": "test_user_1",
            "action": AllowanceActions.SUBMIT.value,
        }
    )

    # Try to create duplicate
    allowance_data = {
        "username": "test_user_1",
        "action": AllowanceActions.SUBMIT.value,
    }
    rv = api_user_client.request(
        "POST",
        "/allowances",
        allowance_data
    )

    assert rv.status_code == status.HTTP_409_CONFLICT


def test_delete_allowance(api_user_client):
    """Test deleting an allowance."""
    mdb = get_mongo_db()
    allowances_collection = mdb.get_collection("_runtime.api.allow")

    # Create allowance to delete
    allowances_collection.insert_one(
        {
            "username": "test_user_1",
            "action":  AllowanceActions.SUBMIT.value,
        }
    )

    allowance_data = {
        "username": "test_user_1",
        "action":  AllowanceActions.SUBMIT.value,
    }

    assert ( allowances_collection.count_documents(allowance_data) == 1)
    # Delete the allowance
    rv = api_user_client.request(
        "DELETE",
        f"/allowances?username=test_user_1&action={AllowanceActions.SUBMIT.value}"
    )

    assert rv.status_code == status.HTTP_204_NO_CONTENT

    # Verify it was deleted from the database
    db_allowance = allowances_collection.find_one(
        {
            "username": "test_user_1",
            "action": AllowanceActions.SUBMIT.value,
        }
    )
    assert db_allowance is None


def test_delete_nonexistent_allowance(api_user_client):
    """Test that deleting a non-existent allowance returns a 404 error."""

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        api_user_client.request(
            "DELETE",
            f"/allowances?username=nonexistent&action={AllowanceActions.SUBMIT.value}"
        )

    assert exc_info.value.response.status_code == status.HTTP_404_NOT_FOUND


def test_list_valid_actions(api_user_client):
    """Test listing all valid actions."""
    # Get valid actions
    rv = api_user_client.request(
        "GET",
        f"/allowances/actions",
    )

    assert rv.status_code == status.HTTP_200_OK
    actions = rv.json()
    # Should include our test actions plus any default ones
    for action in AllowanceActions:
        assert action.value in actions
    # Should be sorted
    assert actions == sorted(actions)


def test_delete_allowance_missing_parameters(api_user_client):
    """Test that delete endpoint requires both username and action parameters."""

    # Try to delete without parameters
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            api_user_client.request(
                "DELETE",
                "/allowances"
            )

    assert exc_info.value.response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
