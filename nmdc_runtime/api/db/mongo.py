import os
from contextlib import AbstractContextManager
from functools import lru_cache
from typing import Set, Dict, Any, Iterable
from uuid import uuid4

from linkml_runtime import SchemaView
from nmdc_schema.nmdc_data import get_nmdc_schema_definition
from pymongo.errors import OperationFailure, AutoReconnect
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel, conint
from tenacity import wait_random_exponential, retry, retry_if_exception_type
from toolz import concat, merge, unique, dissoc

from nmdc_runtime.util import (
    get_nmdc_jsonschema_dict,
    schema_collection_names_with_id_field,
)
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
    """
    Returns set of names of collections that exist in both the nmdc schema and MongoDB database
    """
    collection_names = set()
    schema_view = SchemaView(get_nmdc_schema_definition())
    for slot_name in schema_view.class_slots("Database"):
        slot_definition = schema_view.induced_slot(slot_name, "Database")

        # If this slot doesn't represent a Mongo collection, abort this iteration.
        if not (slot_definition.multivalued and slot_definition.inlined_as_list):
            continue

        collection_names.add(slot_name)

    # filter out collections that exist in schema but not in mongo database
    return collection_names & set(mdb.list_collection_names())


@lru_cache
def activity_collection_names(mdb: MongoDatabase) -> Set[str]:
    return nmdc_schema_collection_names(mdb) - {
        "biosample_set",
        "study_set",
        "data_object_set",
        "functional_annotation_set",
        "genome_feature_set",
    }
