from functools import lru_cache
import os

import pymongo.database
from pymongo import MongoClient


@lru_cache
def get_mongo_db() -> pymongo.database.Database:
    _client = MongoClient(
        host=os.getenv("MONGO_HOST"),
        username=os.getenv("MONGO_USERNAME"),
        password=os.getenv("MONGO_PASSWORD"),
    )
    return _client[os.getenv("MONGO_DBNAME")]
