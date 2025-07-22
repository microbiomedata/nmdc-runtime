import pytest
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.main import ensure_default_api_perms

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
    # assert that the admin user has the expected allowances
    allowances_after = list(allowances_collection.find({}))

    expected_allowances = set(
        ["/metadata/changesheets:submit", "/queries:run(query_cmd:DeleteCommand)", "/queries:run(query_cmd:AggregateCommand)", "/metadata/json:submit"]
    )
    actual_allowances = set(
        allowance["action"] for allowance in allowances_after if allowance["username"] == "admin"
    )
    assert actual_allowances == expected_allowances, "Admin user should have the expected allowances"
