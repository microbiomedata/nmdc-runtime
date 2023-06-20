from uuid import uuid4

import pytest
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError, DuplicateKeyError
from toolz import dissoc

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.util import (
    all_docs_have_unique_id,
    OverlayDB,
    OverlayDBError,
)


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
        odb.delete(coll.name, [{"q": {"id": 0}}])
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
