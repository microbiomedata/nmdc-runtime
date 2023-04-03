import os
from functools import lru_cache
from typing import Set

import pymongo.errors
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from tenacity import wait_random_exponential, retry, retry_if_exception_type

from nmdc_runtime.util import get_nmdc_jsonschema_dict
from pymongo import MongoClient
from pymongo.database import Database as MongoDatabase


@retry(
    retry=retry_if_exception_type(pymongo.errors.AutoReconnect),
    wait=wait_random_exponential(multiplier=0.5, max=60),
)
def check_mongo_ok_autoreconnect(mdb: MongoDatabase):
    return mdb["_runtime.api.allow"].count_documents({}) >= 0


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
    except pymongo.errors.OperationFailure as e:
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
