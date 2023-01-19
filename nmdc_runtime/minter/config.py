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


@lru_cache()
def minting_service_id():
    return os.getenv("MINTING_SERVICE_ID")


@lru_cache()
def typecodes():
    return [
        {"id": "nmdc:nt-11-gha2fh68", "name": "bsm", "schema_class": "nmdc:Biosample"},
        {"id": "nmdc:nt-11-rb11ex57", "name": "nt", "schema_class": "nmdc:NamedThing"},
    ]


@lru_cache()
def shoulders():
    return [
        {
            "id": "nmdc:nt-11-6weqb260",
            "assigned_to": "nmdc:nt-11-zfj0tv58",
            "name": "11",
        },
    ]


@lru_cache
def services():
    return [{"id": "nmdc:nt-11-zfj0tv58", "name": "central minting service"}]


@lru_cache
def requesters():
    return [
        {"id": "nmdc-runtime"},
    ]


@lru_cache()
def schema_classes():
    return [
        {"id": f"nmdc:{k}"}
        for k, v in get_nmdc_jsonschema_dict()["$defs"].items()
        if "required" in v and "id" in v["required"]
    ] + [{"id": "nmdc:NamedThing"}]
