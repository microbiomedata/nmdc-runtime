from uuid import uuid4

from nmdc_runtime.api.db.mongo import get_session_bound_mongo_db
from nmdc_runtime.mongo_util import SessionBoundDatabase


def test_session_bound_mongo_db():
    mdb = get_session_bound_mongo_db(session=None)
    with mdb.client.start_session() as session:
        session_db = SessionBoundDatabase(mdb, session=session)
        session_collection = session_db[f"test-{uuid4()}"]

        # These methods should accept 'session'
        session_collection.insert_one({"_id": 1, "value": "A"})
        session_collection.find_one(
            {"_id": 1}, session=session
        )  # Explicitly passing still works

        # Let's try a method that might not take 'session' (though many do)
        index_name = session_collection.create_index("value")
        print(f"Created index: {index_name}")

        # Operations within the session
        result_in_session = session_collection.find_one({"_id": 1})
        print(f"Find one (with session): {result_in_session}")
        print(
            f"Indexes in session collection: {session_collection.index_information()}"
        )
