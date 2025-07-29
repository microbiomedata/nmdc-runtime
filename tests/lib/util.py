from uuid import uuid4

import pytest

from nmdc_runtime.api.db.mongo import get_session_bound_mongo_db


@pytest.fixture
def rollback_session():
    """
    This fixture provides a session that will be aborted at the end of the test, to clean up any written data.
    """
    session = get_session_bound_mongo_db(session=None).client.start_session()
    session.start_transaction()
    try:
        yield session
    finally:
        session.end_session()  # Automatically aborts any started, uncommitted transaction.


@pytest.fixture
def mdb(rollback_session):
    """Returns a `SessionBoundDatabase` to the MongoDB database specified by environment variables."""
    return get_session_bound_mongo_db(session=rollback_session)


@pytest.fixture
def test_mdb():
    tmp_db_name = f"test-{uuid4()}"
    mdb = get_session_bound_mongo_db(session=None)
    yield mdb.client.get_database(tmp_db_name)
    mdb.client.drop_database(tmp_db_name)
