from nmdc_runtime.mongo_util import get_runtime_mdb
from nmdc_runtime.minter.adapters.repository import MongoIDStore
from nmdc_runtime.minter import config


async def bootstrap():
    mdb = await get_runtime_mdb()
    s = MongoIDStore(mdb)

    async def replace_docs(docs, collection_name):
        for d in docs:
            await s.mdb.raw["minter." + collection_name].replace_one(
                {"id": d["id"]}, d, upsert=True
            )

    await replace_docs(config.typecodes(), "typecodes")
    await replace_docs(config.shoulders(), "shoulders")
    await replace_docs(config.services(), "services")
    await replace_docs(await config.requesters(), "requesters")
    await replace_docs(config.schema_classes(), "schema_classes")

    await refresh_minter_requesters_from_sites()


async def refresh_minter_requesters_from_sites():
    mdb = await get_runtime_mdb()
    s = MongoIDStore(mdb)
    site_ids = [d["id"] async for d in mdb.raw["sites"].find({}, {"id": 1})]
    for sid in site_ids:
        await s.mdb.raw["minter.requesters"].replace_one(
            {"id": sid}, {"id": sid}, upsert=True
        )
