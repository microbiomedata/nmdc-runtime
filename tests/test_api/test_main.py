import pytest
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.main import ensure_default_api_perms
from nmdc_runtime.api.models.allowance import AllowanceAction

@pytest.fixture
def test_db():
    mdb = get_mongo_db()
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    # dump into a list
    original_allowances = list(allowances_collection.find({}))
    # empty the collection
    allowances_collection.delete_many({})
    
    yield mdb

    # empty the collection
    allowances_collection.delete_many({})

    allowances_collection.insert_many(original_allowances)


def test_admin_user_perms(test_db):
    """Test that the admin user has all the expected allowances."""
    
    mdb = test_db
    # get the collection
    allowances_collection = mdb.get_collection("_runtime.api.allow")
    
    # call the function to ensure default API permissions
    ensure_default_api_perms()

    # Confirm the admin user is allowed to perform all actions.
    allowances_after = list(allowances_collection.find({"username": "admin"}))
    expected_actions = set(a.value for a in AllowanceAction)
    actual_actions = set(allowance["action"] for allowance in allowances_after)
    assert actual_actions == expected_actions
