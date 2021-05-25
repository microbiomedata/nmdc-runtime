import os

import pymongo.database
from pymongo import MongoClient

from nmdc_runtime.api.core.auth import get_password_hash

_state = {"db": None}


def _ensure_bootstrap(db: pymongo.database.Database):
    username = os.getenv("API_ADMIN_USER")
    admin_ok = db.users.count_documents(({"username": username})) == 1
    if not admin_ok:
        db.users.insert_one(
            {
                "username": username,
                "hashed_password": get_password_hash(os.getenv("API_ADMIN_PASS")),
            }
        )
        db.users.create_index("username")
    site_id = os.getenv("API_SITE_ID")
    runtime_site_ok = db.sites.count_documents(({"id": site_id})) == 1
    if not runtime_site_ok:
        db.sites.insert_one({"id": site_id})
        db.users.create_index("id")


async def get_mongo_db():
    if _state["db"] is None:
        _client = MongoClient(
            host=os.getenv("MONGO_HOST"),
            username=os.getenv("MONGO_USERNAME"),
            password=os.getenv("MONGO_PASSWORD"),
        )
        db = _client[os.getenv("MONGO_DBNAME")]
        _ensure_bootstrap(db)
        _state["db"] = db

    return _state["db"]
