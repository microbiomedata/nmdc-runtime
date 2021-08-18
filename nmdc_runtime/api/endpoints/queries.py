from datetime import datetime, timezone
from typing import List

import pymongo
from fastapi import APIRouter, Depends, status
from starlette.responses import JSONResponse
from toolz import assoc, merge

from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.query import Query, QueryBase

router = APIRouter()


@router.post("/queries:new", response_model=Query)
def run_new_query(
    query_base: QueryBase,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    qyid = generate_one_id(mdb, "qy")
    # TODO run query!
    created_at = datetime.now(timezone.utc)
    query = Query(
        **query_base.dict(),
        id=qyid,
        created_at=created_at,
        last_ran=created_at,
        result="OK"
    )
    mdb.queries.insert_one(query.dict())
    return query


@router.get("/queries/{query_id}", response_model=Query)
def get_query(
    query_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    doc = mdb.queries.find_one({"id": query_id})
    if not doc:
        return JSONResponse(content={}, status_code=status.HTTP_404_NOT_FOUND)
    return doc


@router.get("/queries", response_model=List[Query])
def list_queries(
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return list(mdb.queries.find())


@router.post("/queries/{query_id}:save", response_model=Query)
def save_query(
    query_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):

    # TODO mechanism to periodically run query and save result as new object iff new result is
    #      different than last saved result? Just save result's sha-256 hash! And if new result,
    #      can add tags to existing objects in order to trigger workflow consideration.
    #      Can saved queries be claimable jobs?

    doc = mdb.queries.find_one({"id": query_id})
    if not doc:
        return JSONResponse(content={}, status_code=status.HTTP_404_NOT_FOUND)
    query = Query(**assoc(doc, "save", True))
    mdb.queries.replace_one({"id": query_id}, query.dict())
    return query


@router.post("/queries/{query_id}:run", response_model=Query)
def run_query(
    query_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    doc = mdb.queries.find_one({"id": query_id})
    if not doc:
        return JSONResponse(content={}, status_code=status.HTTP_404_NOT_FOUND)
    # TODO run query!
    last_ran = datetime.now(timezone.utc)
    query = Query(**merge(doc, dict(last_ran=last_ran, result="OK Again")))
    mdb.queries.replace_one({"id": query_id}, query.dict())
    return query
