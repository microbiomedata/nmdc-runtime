import logging

import pymongo
from bson import json_util
from fastapi import HTTPException
from starlette import status
from toolz import merge

from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.models.util import ListRequest


def list_resources(
    req: ListRequest, mdb: pymongo.database.Database, collection_name: str
):
    limit = req.max_page_size
    filter_ = json_util.loads(req.filter) if req.filter else {}
    if req.page_token:
        doc = mdb.page_tokens.find_one({"_id": req.page_token, "ns": collection_name})
        if doc is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Bad page_token"
            )
        last_id = doc["last_id"]
        mdb.page_tokens.delete_one({"_id": req.page_token})
    else:
        last_id = None
    if last_id is not None:
        if "id" in filter_:
            filter_["id"] = merge(filter_["id"], {"$gt": last_id})
        else:
            filter_ = merge(filter_, {"id": {"$gt": last_id}})

    if mdb[collection_name].count_documents(filter=filter_) <= limit:
        rv = {"resources": list(mdb[collection_name].find(filter=filter_))}
        print(rv)
        return rv
    else:
        if "id_1" not in mdb[collection_name].index_information():
            logging.warning(
                f"list_resources: no index set on 'id' for collection {collection_name}"
            )
        resources = list(
            mdb[collection_name].find(filter=filter_, limit=limit, sort=[("id", 1)])
        )
        last_id = resources[-1]["id"]
        token = generate_one_id(mdb, "page_tokens")
        mdb.page_tokens.insert_one(
            {"_id": token, "ns": collection_name, "last_id": last_id}
        )
        return {"resources": resources, "next_page_token": token}


def exists(collection: pymongo.collection.Collection, filter_: dict):
    return collection.count_documents(filter_) > 0
