from uuid import uuid4

import pytest
from toolz import dissoc

from nmdc_runtime.api.db.mongo import (
    check_mongo_ok_autoreconnect,
    get_mongo_db,
    get_mongo_client,
    OverlayDBError,
    OverlayDB,
)
from nmdc_runtime.util import (
    all_docs_have_unique_id,
)


def _new_collection(mdb):
    return mdb.create_collection(f"test-{uuid4()}")


@pytest.fixture
def mongo_client():
    r"""Yields a `MongoClient` instance configured to access the MongoDB server specified via environment variables."""
    yield get_mongo_client()


@pytest.fixture
def test_db():
    mdb = get_mongo_db()
    tmp_db_name = f"test-{uuid4()}"
    yield mdb.client.get_database(tmp_db_name)
    mdb.client.drop_database(tmp_db_name)


@pytest.fixture
def empty_collection(test_db):
    return _new_collection(test_db)


@pytest.fixture
def none_with_id_collection(test_db):
    coll = _new_collection(test_db)
    coll.insert_many([{"a": n} for n in range(10)])
    return coll


@pytest.fixture
def some_with_id_collection(test_db):
    coll = _new_collection(test_db)
    coll.insert_many([{"id": n} for n in range(10)])
    coll.insert_many([{"a": n} for n in range(10)])
    return coll


@pytest.fixture
def all_with_unique_id_collection(test_db):
    coll = _new_collection(test_db)
    coll.insert_many([{"id": n} for n in range(10)])
    return coll


@pytest.fixture
def all_with_nonunique_id_collection(test_db):
    coll = _new_collection(test_db)
    coll.insert_many([{"id": 1} for _ in range(10)])
    return coll


def test_all_docs_have_unique_id(
    empty_collection,
    none_with_id_collection,
    some_with_id_collection,
    all_with_unique_id_collection,
    all_with_nonunique_id_collection,
):
    assert all_docs_have_unique_id(empty_collection) is False
    assert all_docs_have_unique_id(none_with_id_collection) is False
    assert all_docs_have_unique_id(some_with_id_collection) is False
    assert all_docs_have_unique_id(all_with_unique_id_collection) is True
    assert all_docs_have_unique_id(all_with_nonunique_id_collection) is False


def test_overlaydb_apply_updates(test_db):
    coll = _new_collection(test_db)
    coll.insert_many([{"id": n} for n in range(10)])
    with OverlayDB(test_db) as odb:
        rv = [
            dissoc(d, "_id") for d in odb.merge_find(coll.name, {"filter": {"id": 0}})
        ]
        assert rv[0] == {"id": 0}
        odb.apply_updates(coll.name, [{"q": {"id": 0}, "u": {"$set": {"foo": "bar"}}}])
        rv = [
            dissoc(d, "_id") for d in odb.merge_find(coll.name, {"filter": {"id": 0}})
        ]
        assert rv[0] == {"id": 0, "foo": "bar"}


def test_overlaydb_delete(test_db):
    coll = _new_collection(test_db)
    coll.insert_many([{"id": n} for n in range(10)])
    with OverlayDB(test_db) as odb:
        rv = [
            dissoc(d, "_id") for d in odb.merge_find(coll.name, {"filter": {"id": 0}})
        ]
        assert rv[0] == {"id": 0}
        odb.delete(coll.name, [{"q": {"id": 0}, "limit": 1}])
        rv = [
            dissoc(d, "_id") for d in odb.merge_find(coll.name, {"filter": {"id": 0}})
        ]
        assert rv == []
        assert len([dissoc(d, "_id") for d in odb.merge_find(coll.name, {})]) == 9


def test_overlaydb_duplicate_ids_replace_or_insert_many(test_db):
    with OverlayDB(test_db) as odb:
        with pytest.raises(OverlayDBError):
            odb.replace_or_insert_many(
                "field_research_site_set",
                [
                    {"id": "nmdc:frsite-11-s2dqk408", "name": "BESC-470-CL2_38_23"},
                    {"id": "nmdc:frsite-11-s2dqk408", "name": "BESC-470-CL2_38_23"},
                    {"id": "nmdc:frsite-11-s2dqk408", "name": "BESC-470-CL2_38_23"},
                ],
            )


def test_overlaydb_replace_or_insert_many(test_db):
    coll = _new_collection(test_db)
    coll.insert_many([{"id": n} for n in range(10)])
    with OverlayDB(test_db) as odb:
        odb.replace_or_insert_many(coll.name, [{"id": n} for n in range(20)])
        assert len(list(odb.merge_find(coll.name, {}))) == 20


def test_mongo_client_supports_transactions(mongo_client):
    r"""
    Note: This test was written to demonstrate how MongoDB sessions and transactions work.
          It does not exercise any parts of the Runtime.

    Reference: https://pymongo.readthedocs.io/en/stable/api/pymongo/client_session.html#transactions
    """

    # Create a test database containing an empty collection.
    db_name = f"test-{uuid4()}"
    collection_name = "my_collection"
    db = mongo_client.get_database(db_name)
    collection = db.create_collection(collection_name)
    assert len(db.list_collection_names()) == 1

    # Insert documents into that collection (notice the three food names begin with the letters "a", "b", and "c").
    collection.insert_one({"food": "apple"})
    collection.insert_one({"food": "banana"})
    collection.insert_one({"food": "carrot"})
    assert collection.count_documents({}) == 3

    # Start a session, so we can eventually start a transaction.
    with mongo_client.start_session() as session:

        # Modify the document while using the _session_ and confirm the real database _is_ affected.
        collection.update_one(
            {"food": "apple"}, {"$set": {"food": "donut"}}, session=session
        )
        assert set(collection.distinct("food", session=session)) == {
            "donut",
            "banana",
            "carrot",
        }
        assert set(collection.distinct("food")) == {
            "donut",
            "banana",
            "carrot",
        }  # immediately says "donut"

        # Start a transaction.
        with session.start_transaction():

            # Modify the document within the _transaction_ and confirm the real database is _not_ affected.
            collection.update_one(
                {"food": "banana"}, {"$set": {"food": "egg"}}, session=session
            )
            assert set(collection.distinct("food", session=session)) == {
                "donut",
                "egg",
                "carrot",
            }  # says "egg"
            assert set(collection.distinct("food")) == {
                "donut",
                "banana",
                "carrot",
            }  # still says "banana"

            # Abort the transaction.
            #
            # Note: If an exception had been raised within this currently-pending transaction, PyMongo would have
            #       invoked this function implicitly. Here, we invoke it explicitly instead of raising an exception.
            #
            session.abort_transaction()

        # Confirm the real database—whether using or not using the session—does _not_ reflect the changes that were
        # made within the aborted transaction.
        assert set(collection.distinct("food", session=session)) == {
            "donut",
            "banana",
            "carrot",
        }  # back to "banana"
        assert set(collection.distinct("food")) == {"donut", "banana", "carrot"}

    # Confirm the real database _does_ reflect the changes that were made using the now-ended session.
    assert set(collection.distinct("food")) == {"donut", "banana", "carrot"}

    # Clean up.
    mongo_client.drop_database(db_name)


def test_check_mongo_ok_autoreconnect(test_db):
    assert check_mongo_ok_autoreconnect(mdb=test_db) is True
