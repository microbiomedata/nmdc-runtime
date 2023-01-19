import os
from functools import lru_cache

from nmdc_schema.nmdc_data import get_nmdc_jsonschema_dict
from pymongo import MongoClient
from pymongo.database import Database as MongoDatabase

from nmdc_runtime.minter.adapters.repository import InMemoryIDStore
from nmdc_runtime.minter.domain.model import MintingRequest, Identifier

TYPECODES = [
    {"id": "nmdc:nt-11-gha2fh68", "name": "bsm", "schema_class": "nmdc:Biosample"},
    {"id": "nmdc:nt-11-rb11ex57", "name": "nt", "schema_class": "nmdc:NamedThing"},
]
SHOULDERS = [
    {"id": "nmdc:nt-11-6weqb260", "assigned_to": "nmdc:nt-11-zfj0tv58", "name": "11"},
]
SERVICES = [{"id": "nmdc:nt-11-zfj0tv58", "name": "central minting service"}]
REQUESTERS = [{"id": "nmdc:pers-11-pm7mfv46", "name": "Alicia"}]

SCHEMA_CLASSES = [
    {"id": f"nmdc:{k}"}
    for k, v in get_nmdc_jsonschema_dict()["$defs"].items()
    if "required" in v and "id" in v["required"]
]


def minting_request():
    return MintingRequest(
        **{
            "service": {"id": "nmdc:nt-11-zfj0tv58"},
            "requester": {"id": "nmdc:pers-11-pm7mfv46"},
            "schema_class": {"id": "nmdc:Biosample"},
            "how_many": 1,
        }
    )


def draft_identifier():
    id_ = "nmdc:nt-11-z8x8p723"
    return Identifier(
        **{
            "id": id_,
            "name": id_,
            "typecode": {"id": next(d["id"] for d in TYPECODES if d["name"] == "nt")},
            "shoulder": {"id": next(d["id"] for d in SHOULDERS if d["name"] == "11")},
            "status": "draft",
        }
    )


@lru_cache
def get_mongo_test_db() -> MongoDatabase:
    _client = MongoClient(
        host=os.getenv("MONGO_HOST"),
        username=os.getenv("MONGO_USERNAME"),
        password=os.getenv("MONGO_PASSWORD"),
    )
    db: MongoDatabase = _client[os.getenv("MONGO_TEST_DBNAME")]

    db.typecodes.drop()
    db.typecodes.insert_many(TYPECODES)

    db.shoulders.drop()
    db.shoulders.insert_many(SHOULDERS)

    db.services.drop()
    db.services.insert_many(SERVICES)

    db.requesters.drop()
    db.requesters.insert_many(REQUESTERS)

    db.schema_classes.drop()
    db.schema_classes.insert_many(SCHEMA_CLASSES)

    return db


@lru_cache()
def get_test_inmemoryidstore() -> InMemoryIDStore:
    return InMemoryIDStore(
        services=SERVICES,
        shoulders=SHOULDERS,
        typecodes=TYPECODES,
        requesters=REQUESTERS,
        schema_classes=SCHEMA_CLASSES,
    )
