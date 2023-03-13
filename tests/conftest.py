import os
from functools import lru_cache

from nmdc_runtime.api.core.util import import_via_dotted_path
from nmdc_runtime.util import get_nmdc_jsonschema_dict
from pymongo import MongoClient
from pymongo.database import Database as MongoDatabase

from nmdc_runtime.minter.adapters.repository import InMemoryIDStore
from nmdc_runtime.minter.config import (
    typecodes,
    shoulders,
    services,
    requesters,
    schema_classes,
)
from nmdc_runtime.minter.domain.model import MintingRequest, Identifier


def minting_request():
    return MintingRequest(
        **{
            "service": services()[0],
            "requester": requesters()[0],
            "schema_class": schema_classes()[0],
            "how_many": 1,
        }
    )


def draft_identifier():
    id_ = "nmdc:bsm-11-z8x8p723"
    return Identifier(
        **{
            "id": id_,
            "name": id_,
            "typecode": {
                "id": next(d["id"] for d in typecodes() if d["name"] == "bsm")
            },
            "shoulder": {"id": next(d["id"] for d in shoulders() if d["name"] == "11")},
            "status": "draft",
        }
    )


@lru_cache
def get_mongo_test_db() -> MongoDatabase:
    _client = MongoClient(
        host=os.getenv("MONGO_HOST"),
        username=os.getenv("MONGO_USERNAME"),
        password=os.getenv("MONGO_PASSWORD"),
        directConnection=True,
    )
    db: MongoDatabase = _client[os.getenv("MONGO_TEST_DBNAME")]

    for coll_name in [
        "typecodes",
        "shoulders",
        "services",
        "requesters",
        "schema_classes",
    ]:
        db[f"minter.{coll_name}"].drop()
        db[f"minter.{coll_name}"].insert_many(
            import_via_dotted_path(f"nmdc_runtime.minter.config.{coll_name}")()
        )

    return db


@lru_cache()
def get_test_inmemoryidstore() -> InMemoryIDStore:
    return InMemoryIDStore(
        services=services(),
        shoulders=shoulders(),
        typecodes=typecodes(),
        requesters=requesters(),
        schema_classes=schema_classes(),
    )
