from nmdc_runtime.minter.adapters.repository import MongoIDStore
from nmdc_runtime.minter import config


def ensure_minter_id_records_id_is_indexed():
    """Ensures the `id` field of the `minter.id_records` collection has a unique index."""

    db = config.get_mongo_db()
    minter_id_records = db.get_collection("minter.id_records")
    minter_id_records.create_index(
        "id", background=True, unique=True,
    )


def bootstrap():
    mdb = config.get_mongo_db()
    s = MongoIDStore(mdb)
    ensure_minter_id_records_id_is_indexed()
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
