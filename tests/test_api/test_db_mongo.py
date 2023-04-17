from uuid import uuid4

from pymongo.collection import Collection
import pytest

from nmdc_runtime.api.db.mongo import get_mongo_db, all_docs_have_unique_id


def _new_collection(mdb):
    return mdb.create_collection(f"test-{uuid4()}")


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
