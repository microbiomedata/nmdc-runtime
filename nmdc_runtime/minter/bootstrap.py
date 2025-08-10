from nmdc_runtime.minter.adapters.repository import MongoIDStore
from nmdc_runtime.minter import config


def bootstrap():
    mdb = config.get_mongo_db()
    s = MongoIDStore(mdb)
    for collection_name in [
        "typecodes",
        "shoulders",
        "services",
        "requesters",
        "schema_classes",
    ]:
        for d in getattr(config, collection_name)():
            s.db["minter." + collection_name].replace_one(
                {"id": d["id"]}, d, upsert=True
            )
    refresh_minter_requesters_from_sites()


def refresh_minter_requesters_from_sites():
    mdb = config.get_mongo_db()
    s = MongoIDStore(mdb)
    site_ids = [d["id"] for d in mdb.sites.find({}, {"id": 1})]
    for sid in site_ids:
        s.db["minter.requesters"].replace_one({"id": sid}, {"id": sid}, upsert=True)
