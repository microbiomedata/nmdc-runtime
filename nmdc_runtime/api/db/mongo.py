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
