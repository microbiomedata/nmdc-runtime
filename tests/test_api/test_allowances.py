import pytest
import requests
from starlette import status

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.allowance import AllowanceAction


@pytest.fixture(autouse=True)
def cleanup_test_allowances(api_user_client):
    """Cleanup test allowances before and after each test."""
    mdb = get_mongo_db()

    # Store the initial allowances so we can restore them later.
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    allowances_docs_initial = list(allowances_collection.find({}))
    allowances_collection.delete_many({})

    # Allow the `api_user_client` to manage allowances.
    allowances_collection.insert_one({
        "username": api_user_client.username,
        "action": AllowanceAction.MANAGE_ALLOWANCES.value,
    })

    yield

    # Restore the collection to its original state.
    allowances_collection.delete_many({})
    allowances_collection.insert_many(allowances_docs_initial)


def test_list_all_allowances(api_user_client):
    """Test listing all allowances."""
    mdb = get_mongo_db()
    
    # Create some allowances.
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    test_allowances = [
        {"username": "user_1", "action": AllowanceAction.AGGREGATE_DATA.value},
        {"username": "user_2", "action": AllowanceAction.DELETE_DATA.value},
    ]
    allowances_collection.insert_many(test_allowances)
    
    # Get all allowances.
    rv = api_user_client.request(
        "GET",
        "/admin/allowances",
    )

    assert rv.status_code == status.HTTP_200_OK
    response_payload = rv.json()
    allowances_received = response_payload["resources"]
    assert len(allowances_received) == 3  # 2 from test + 1 from fixture


def test_list_allowances_by_username(api_user_client):
    """Test listing allowances filtered by username."""
    mdb = get_mongo_db()

    # Create some allowances.
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    allowances_collection.insert_many(
        [
            {"username": "user_1", "action": AllowanceAction.AGGREGATE_DATA.value},
            {"username": "user_1", "action": AllowanceAction.DELETE_DATA.value},
            {"username": "user_2", "action": AllowanceAction.SUBMIT_CHANGESHEETS.value},
        ]
    )

    # Get allowances for user_1.
    rv = api_user_client.request(
        "GET",
        "/admin/allowances?username=user_1",
    )

    assert rv.status_code == status.HTTP_200_OK
    response_payload = rv.json()
    allowances_received = response_payload["resources"]
    assert len(allowances_received) == 2
    assert all(a["username"] == "user_1" for a in allowances_received)


def test_list_allowances_by_action(api_user_client):
    """Test listing allowances filtered by action."""
    mdb = get_mongo_db()

    # Create some allowances.
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    allowances_collection.insert_many(
        [
            {"username": "user_1", "action": AllowanceAction.SUBMIT_CHANGESHEETS.value},
            {"username": "user_2", "action": AllowanceAction.SUBMIT_CHANGESHEETS.value},
            {"username": "user_3", "action": AllowanceAction.DELETE_DATA.value},
        ]
    )

    # Get allowances for `AllowanceAction.SUBMIT_CHANGESHEET`.
    rv = api_user_client.request(
        "GET",
        f"/admin/allowances?action={AllowanceAction.SUBMIT_CHANGESHEETS.value}",
    )

    assert rv.status_code == status.HTTP_200_OK
    response_payload = rv.json()
    allowances_received = response_payload["resources"]
    assert len(allowances_received) == 2
    assert all(a["action"] == AllowanceAction.SUBMIT_CHANGESHEETS.value for a in allowances_received)


def test_list_allowance_by_username_and_action(api_user_client):
    """Test listing specific allowance by both username and action."""
    mdb = get_mongo_db()

    # Create some allowances.
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    allowances_collection.insert_many(
        [
            {"username": "user_1", "action": AllowanceAction.SUBMIT_CHANGESHEETS.value},
            {"username": "user_1", "action": AllowanceAction.DELETE_DATA.value},
        ]
    )

    # Get specific allowance.
    rv = api_user_client.request(
        "GET",
        f"/admin/allowances?username=user_1&action={AllowanceAction.SUBMIT_CHANGESHEETS.value}",
    )

    assert rv.status_code == status.HTTP_200_OK
    response_payload = rv.json()
    allowances_received = response_payload["resources"]
    assert len(allowances_received) == 1
    assert allowances_received[0]["username"] == "user_1"
    assert allowances_received[0]["action"] == AllowanceAction.SUBMIT_CHANGESHEETS.value


def test_create_allowance(api_user_client):
    """Test creating a new allowance."""
    mdb = get_mongo_db()

    # Define the allowance we will create.
    allowance_to_create = {
        "username": "user_1",
        "action": AllowanceAction.SUBMIT_CHANGESHEETS.value,
    }

    # Assert that no such allowance already exists.
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    assert allowances_collection.count_documents(allowance_to_create) == 0

    # Create the allowance.
    rv = api_user_client.request(
        "POST",
        "/admin/allowances",
        allowance_to_create
    )

    assert rv.status_code == status.HTTP_201_CREATED
    created_allowance = rv.json()
    assert created_allowance["username"] == allowance_to_create["username"]
    assert created_allowance["action"] == allowance_to_create["action"]

    # Verify it exists in the database now.
    assert allowances_collection.count_documents(allowance_to_create) == 1


def test_create_duplicate_allowance(api_user_client):
    """Test that creating a duplicate allowance returns a conflict error."""
    mdb = get_mongo_db()

    # Define the allowance we will create.
    allowance_to_create = {
        "username": "user_1",
        "action": AllowanceAction.SUBMIT_CHANGESHEETS.value,
    }

    # Insert the allowance into the database.
    #
    # Note: The `insert_one` call will add an `_id` field to the dictionary passed in,
    #       so we cannot reuse that dictionary as-is with the subsequent API request.
    #
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    allowances_collection.insert_one(allowance_to_create)

    # Try to create an identical allowance via the API.
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        _ = api_user_client.request(
            "POST",
            "/admin/allowances",
            {
                "username": allowance_to_create["username"],
                "action": allowance_to_create["action"],
            }
        )
    assert exc_info.value.response.status_code == status.HTTP_409_CONFLICT


def test_delete_allowance(api_user_client):
    """Test deleting an allowance."""
    mdb = get_mongo_db()

    # Define the allowance we will delete.
    allowance_to_delete = {
        "username": "user_1",
        "action": AllowanceAction.SUBMIT_CHANGESHEETS.value,
    }

    # Create the allowance we will delete.
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    allowances_collection.insert_one(allowance_to_delete)
    assert allowances_collection.count_documents(allowance_to_delete) == 1

    # Delete the allowance via the API.
    username = allowance_to_delete['username']
    action = allowance_to_delete['action']
    rv = api_user_client.request(
        "DELETE",
        f"/admin/allowances?username={username}&action={action}"
    )

    assert rv.status_code == status.HTTP_204_NO_CONTENT

    # Verify it was deleted from the database.
    assert allowances_collection.count_documents(allowance_to_delete) == 0


def test_delete_nonexistent_allowance(api_user_client):
    """Test that deleting a non-existent allowance returns a 404 error."""
    mdb = get_mongo_db()

    # Define the allowance we will try to delete.
    allowance_to_delete = {
        "username": "user_1",
        "action": AllowanceAction.SUBMIT_CHANGESHEETS.value,
    }

    # Confirm no such allowance exists.
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    assert allowances_collection.count_documents(allowance_to_delete) == 0

    # Confirm trying to delete it via the API results in an error response.
    username = allowance_to_delete['username']
    action = allowance_to_delete['action']
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        api_user_client.request(
            "DELETE",
            f"/admin/allowances?username={username}&action={action}"
        )

    assert exc_info.value.response.status_code == status.HTTP_404_NOT_FOUND


def test_list_valid_actions(api_user_client):
    """Test listing all valid actions."""

    # Get all valid allowance actions.
    rv = api_user_client.request(
        "GET",
        "/admin/allowances/actions",
    )

    assert rv.status_code == status.HTTP_200_OK
    actions_received = rv.json()
    
    # Confirm it's the same set of actions as is described by our enum.
    assert set(actions_received) == set(a.value for a in AllowanceAction)


def test_delete_allowance_missing_parameters(api_user_client):
    """Test that delete endpoint requires both username and action parameters."""

    # Try to delete without parameters
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        api_user_client.request(
            "DELETE",
            "/admin/allowances?username=user_1"
        )

    assert exc_info.value.response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
