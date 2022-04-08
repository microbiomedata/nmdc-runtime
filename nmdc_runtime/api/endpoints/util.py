import logging
import re
from time import time_ns
from typing import Set

import pymongo
from bson import json_util
from fastapi import HTTPException
from starlette import status
from toolz import merge, dissoc, concat

from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.models.util import ListRequest, FindRequest


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


def maybe_unstring(val):
    try:
        return float(val)
    except ValueError:
        return val


def get_mongo_filter(filter_str):
    filter_ = {}
    if not filter_str:
        return filter_

    pairs = re.split(r"\s*,\s*", filter_str)  # comma, perhaps surrounded by whitespace

    if not all(len(split) == 2 for split in (p.split(":", maxsplit=1) for p in pairs)):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Filter must be of form: attribute:spec[,attribute:spec]*",
        )

    for attr, spec in (p.split(":", maxsplit=1) for p in pairs):
        if attr.endswith(".search"):
            actual_attr = attr[: -len(".search")]
            filter_[actual_attr] = {"$regex": spec}
        else:
            for op, key in {("<", "$lt"), ("<=", "$lte"), (">", "$gt"), (">=", "$gte")}:
                if spec.startswith(op):
                    filter_[attr] = {key: maybe_unstring(spec[len(op) :])}
                    break
            else:
                filter_[attr] = spec
    return filter_


def strip_oid(doc):
    return dissoc(doc, "_id")


def timeit(cursor):
    """Collect from cursor and return time taken in milliseconds."""
    tic = time_ns()
    results = list(cursor)
    toc = time_ns()
    return results, int(round((toc - tic) / 1e6))


def find_resources(
    req: FindRequest, mdb: pymongo.database.Database, collection_name: str
):
    filter_ = get_mongo_filter(req.filter)

    if req.cursor and req.page:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="cannot use cursor- and page-based pagination together",
        )
    elif (not req.cursor) and (not req.page):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="find query needs to request one of page or cursor",
        )

    total_count = mdb[collection_name].count_documents(filter=filter_)

    if req.page:
        skip = (req.page - 1) * req.per_page
        if skip > 10_000:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Use cursor-based pagination for paging beyond 10,000 items",
            )
        limit = req.per_page
        results, db_response_time_ms = timeit(
            mdb[collection_name].find(filter=filter_, skip=skip, limit=limit)
        )
        rv = {
            "meta": {
                "mongo_filter_dict": filter_,
                "count": total_count,
                "db_response_time_ms": db_response_time_ms,
                "page": req.page,
                "per_page": req.per_page,
            },
            "results": [strip_oid(d) for d in results],
            "group_by": [],
        }

    else:  # req.cursor is not None
        if req.cursor != "*":
            doc = mdb.page_tokens.find_one({"_id": req.cursor, "ns": collection_name})
            if doc is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail="Bad cursor value"
                )
            last_id = doc["last_id"]
            mdb.page_tokens.delete_one({"_id": req.cursor})
        else:
            last_id = None

        if last_id is not None:
            if "id" in filter_:
                filter_["id"] = merge(filter_["id"], {"$gt": last_id})
            else:
                filter_ = merge(filter_, {"id": {"$gt": last_id}})

        if "id_1" not in mdb[collection_name].index_information():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cursor-based pagination is not enabled for this resource.",
            )

        limit = req.per_page
        results, db_response_time_ms = timeit(
            mdb[collection_name].find(filter=filter_, limit=limit, sort=[("id", 1)])
        )
        last_id = results[-1]["id"]
        token = generate_one_id(mdb, "page_tokens")
        mdb.page_tokens.insert_one(
            {"_id": token, "ns": collection_name, "last_id": last_id}
        )
        rv = {
            "meta": {
                "mongo_filter_dict": filter_,
                "count": total_count,
                "db_response_time_ms": db_response_time_ms,
                "page": None,
                "per_page": req.per_page,
                "next_cursor": token,
            },
            "results": [strip_oid(d) for d in results],
            "group_by": [],
        }
    return rv


def find_resources_spanning(
    req: FindRequest, mdb: pymongo.database.Database, collection_names: Set[str]
):
    if req.cursor or not req.page:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="This resource only supports page-based pagination",
        )

    responses = {name: find_resources(req, mdb, name) for name in collection_names}
    rv = {
        "meta": {
            "mongo_filter_dict": next(
                r["meta"]["mongo_filter_dict"] for r in responses.values()
            ),
            "count": sum(r["meta"]["count"] for r in responses.values()),
            "db_response_time_ms": sum(
                r["meta"]["db_response_time_ms"] for r in responses.values()
            ),
            "page": req.page,
            "per_page": req.per_page,
        },
        "results": list(concat(r["results"] for r in responses.values())),
        "group_by": [],
    }
    return rv


def exists(collection: pymongo.collection.Collection, filter_: dict):
    return collection.count_documents(filter_) > 0
