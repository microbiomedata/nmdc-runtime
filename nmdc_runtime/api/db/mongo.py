import os
from functools import lru_cache

from nmdc_schema.nmdc_data import get_nmdc_jsonschema_dict
from pymongo import MongoClient
from pymongo.database import Database as MongoDatabase


@lru_cache
def get_mongo_db() -> MongoDatabase:
    _client = MongoClient(
        host=os.getenv("MONGO_HOST"),
        username=os.getenv("MONGO_USERNAME"),
        password=os.getenv("MONGO_PASSWORD"),
    )
    return _client[os.getenv("MONGO_DBNAME")]


@lru_cache
def nmdc_schema_collection_names(mdb: MongoDatabase):
    names = set(mdb.list_collection_names()) & set(
        get_nmdc_jsonschema_dict()["$defs"]["Database"]["properties"]
    )
    return names - {
        "activity_set",
        "nmdc_schema_version",
        "date_created",
        "etl_software_version",
    }
