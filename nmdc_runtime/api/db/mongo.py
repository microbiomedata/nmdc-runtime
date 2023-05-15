import os
from contextlib import AbstractContextManager
from functools import lru_cache
from typing import Set, Dict, Any, Iterable
from uuid import uuid4

from pymongo.errors import OperationFailure, AutoReconnect
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel, conint
from tenacity import wait_random_exponential, retry, retry_if_exception_type
from toolz import concat, merge, unique, dissoc

from nmdc_runtime.api.core.metadata import schema_collection_names_with_id_field
from nmdc_runtime.util import get_nmdc_jsonschema_dict
from pymongo import MongoClient, ReplaceOne
from pymongo.database import Database as MongoDatabase


@retry(
    retry=retry_if_exception_type(AutoReconnect),
    wait=wait_random_exponential(multiplier=0.5, max=60),
)
def check_mongo_ok_autoreconnect(mdb: MongoDatabase):
    mdb["_runtime.healthcheck"].insert_one({"_id": "ok"})
    mdb["_runtime.healthcheck"].delete_one({"_id": "ok"})
    return True


@lru_cache
def get_mongo_db() -> MongoDatabase:
    _client = MongoClient(
        host=os.getenv("MONGO_HOST"),
        username=os.getenv("MONGO_USERNAME"),
        password=os.getenv("MONGO_PASSWORD"),
        directConnection=True,
    )
    try:
        _client.admin.command("replSetGetConfig")
    except OperationFailure as e:
        if e.details["codeName"] == "NotYetInitialized":
            _client.admin.command("replSetInitiate")

    mdb = _client[os.getenv("MONGO_DBNAME")]
    check_mongo_ok_autoreconnect(mdb)
    return mdb


@lru_cache
def get_async_mongo_db() -> AsyncIOMotorDatabase:
    _client = AsyncIOMotorClient(
        host=os.getenv("MONGO_HOST"),
        username=os.getenv("MONGO_USERNAME"),
        password=os.getenv("MONGO_PASSWORD"),
        directConnection=True,
    )
    return _client[os.getenv("MONGO_DBNAME")]


@lru_cache
def nmdc_schema_collection_names(mdb: MongoDatabase) -> Set[str]:
    names = set(mdb.list_collection_names()) & set(
        get_nmdc_jsonschema_dict()["$defs"]["Database"]["properties"]
    )
    return names - {
        "activity_set",
        "nmdc_schema_version",
        "date_created",
        "etl_software_version",
    }


@lru_cache
def activity_collection_names(mdb: MongoDatabase) -> Set[str]:
    return nmdc_schema_collection_names(mdb) - {
        "biosample_set",
        "study_set",
        "data_object_set",
        "functional_annotation_set",
        "genome_feature_set",
    }


def all_docs_have_unique_id(coll) -> bool:
    first_doc = coll.find_one({}, ["id"])
    if first_doc is None or "id" not in first_doc:
        # short-circuit exit for empty collection or large collection via first-doc peek.
        return False

    total_count = coll.count_documents({})
    return len(coll.distinct("id")) == total_count


def ensure_unique_id_indexes(mdb: MongoDatabase):
    """Ensure that any collections with an "id" field have an index on "id"."""
    candidate_names = (
        set(mdb.list_collection_names()) | schema_collection_names_with_id_field()
    )
    for collection_name in candidate_names:
        if collection_name.startswith("system."):  # reserved by mongodb
            continue

        if (
            collection_name in schema_collection_names_with_id_field()
            or all_docs_have_unique_id(mdb[collection_name])
        ):
            mdb[collection_name].create_index("id", unique=True)


class UpdateStatement(BaseModel):
    q: dict
    u: dict
    upsert: bool = False
    multi: bool = False


class DeleteStatement(BaseModel):
    q: dict
    limit: conint(ge=0, le=1) = 1


class OverlayDBError(Exception):
    pass


class OverlayDB(AbstractContextManager):
    """Provides a context whereby a base Database is overlaid with a temporary one.

    If you need to run basic simulations of updates to a base database,
    you don't want to actually commit transactions to the base database.

    For example, to insert or replace (matching on "id") many documents into a collection in order
    to then validate the resulting total set of collection documents, an OverlayDB writes to
    an overlay collection that "shadows" the base collection during a "find" query
    (the "merge_find" method of an OverlayDB object): if a document with `id0` is found in the
    overlay collection, that id is marked as "seen" and will not also be returned when
    subsequently scanning the (unmodified) base-database collection.

    Mongo "update" commands (as the "apply_updates" method) are simulated by first copying affected
    documents from a base collection to the overlay, and then applying the updates to the overlay,
    so that again, base collections are unmodified, and a "merge_find" call will produce a result
    *as if* the base collection(s) were modified.

    Mongo deletions (as the "delete" method) also copy affected documents from the base collection
    to the overlay collection, and flag them using the "_deleted" field. In this way, a `merge_find`
    call will match a relevant document given a suitable filter, and will mark the document's id
    as "seen" *without* returning the document. Thus, the result is as if the document were deleted.

    Usage:
    ````
    with OverlayDB(mdb) as odb:
        # do stuff, e.g. `odb.replace_or_insert_many(...)`
    ```
    """

    def __init__(self, mdb: MongoDatabase):
        self._bottom_db = mdb
        self._top_db = self._bottom_db.client.get_database(f"overlay-{uuid4()}")
        ensure_unique_id_indexes(self._top_db)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._bottom_db.client.drop_database(self._top_db.name)

    def replace_or_insert_many(self, coll_name, documents: list):
        try:
            self._top_db[coll_name].insert_many(documents)
        except OperationFailure as e:
            raise OverlayDBError(str(e.details))

    def apply_updates(self, coll_name, updates: list):
        """prepare overlay db and apply updates to it."""
        assert all(UpdateStatement(**us) for us in updates)
        for update_spec in updates:
            for bottom_doc in self._bottom_db[coll_name].find(update_spec["q"]):
                self._top_db[coll_name].insert_one(bottom_doc)
        try:
            self._top_db.command({"update": coll_name, "updates": updates})
        except OperationFailure as e:
            raise OverlayDBError(str(e.details))

    def delete(self, coll_name, deletes: list):
        """ "apply" delete command by flagging docs in overlay database"""
        assert all(DeleteStatement(**us) for us in deletes)
        for delete_spec in deletes:
            for bottom_doc in self._bottom_db[coll_name].find(
                delete_spec["q"], limit=delete_spec.get("limit", 1)
            ):
                bottom_doc["_deleted"] = True
                self._top_db[coll_name].insert_one(bottom_doc)

    def merge_find(self, coll_name, find_spec: dict):
        """Yield docs first from overlay and then from base db, minding deletion flags."""
        # ensure projection of "id" and "_deleted"
        if "projection" in find_spec:
            proj = find_spec["projection"]
            if isinstance(proj, dict):
                proj = merge(proj, {"id": 1, "_deleted": 1})
            elif isinstance(proj, list):
                proj = list(unique(proj + ["id", "_deleted"]))

        top_docs = self._top_db[coll_name].find(**find_spec)
        bottom_docs = self._bottom_db[coll_name].find(**find_spec)
        top_seen_ids = set()
        for doc in top_docs:
            if not doc.get("_deleted"):
                yield doc
            top_seen_ids.add(doc["id"])

        for doc in bottom_docs:
            if doc["id"] not in top_seen_ids:
                yield doc
