import os

import pymongo.database
from pymongo import MongoClient

from nmdc_runtime.api.core.auth import get_password_hash

_state = {"db": None}


async def get_mongo_db():
    if _state["db"] is None:
        _client = MongoClient(
            host=os.getenv("MONGO_HOST"),
            username=os.getenv("MONGO_USERNAME"),
            password=os.getenv("MONGO_PASSWORD"),
        )
        db = _client[os.getenv("MONGO_DBNAME")]
        _state["db"] = db

    return _state["db"]
