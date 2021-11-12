import json
from typing import List

import bson.json_util
from fastapi import APIRouter, Depends, status, HTTPException
from pymongo.database import Database as MongoDatabase

from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.core.util import now, raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.query import (
    Query,
    QueryResponseOptions,
    DeleteCommand,
    QueryRun,
    CommandResponse,
    command_response_for,
    QueryCmd,
)
from nmdc_runtime.api.models.user import get_current_active_user, User

router = APIRouter()


def unmongo(d: dict) -> dict:
    """Ensure a dict with e.g. mongo ObjectIds will serialize as JSON."""
    return json.loads(bson.json_util.dumps(d))


def check_can_delete(user: User):
    can_delete = {"dehays", "scanon", "dwinston"}
    if user.username not in can_delete:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"only users {can_delete} are allowed to issue delete commands.",
        )


@router.post("/queries:run", response_model=QueryResponseOptions)
def run_query(
    query_cmd: QueryCmd,
    mdb: MongoDatabase = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    """

    Examples:
    ```
    {
      "find": "biosample_set",
      "filter": {}
    }

    {
      "find": "biosample_set",
      "filter": {"part_of": "gold:Gs0114663"}
    }

    {
      "delete": "biosample_set",
      "deletes": [{"q": {"id": "NOT_A_REAL_ID"}, "limit": 1}]
    }
    ```
    """
    if isinstance(query_cmd, DeleteCommand):
        check_can_delete(user)

    qid = generate_one_id(mdb, "qy")
    saved_at = now()
    query = Query(
        cmd=query_cmd,
        id=qid,
        saved_at=saved_at,
    )
    mdb.queries.insert_one(query.dict(exclude_unset=True))
    cmd_response = _run_query(query, mdb)
    return unmongo(cmd_response.dict(exclude_unset=True))


@router.get("/queries/{query_id}", response_model=Query)
def get_query(
    query_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    return raise404_if_none(mdb.queries.find_one({"id": query_id}))


@router.get("/queries", response_model=List[Query])
def list_queries(
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    return list(mdb.queries.find())


@router.post("/queries/{query_id}:run", response_model=Query)
def rerun_query(
    query_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    doc = raise404_if_none(mdb.queries.find_one({"id": query_id}))
    query = Query(**doc)
    if isinstance(query, DeleteCommand):
        check_can_delete(user)

    cmd_response = _run_query(query, mdb)
    return unmongo(cmd_response.dict(exclude_unset=True))


def _run_query(query, mdb) -> CommandResponse:
    q_type = type(query.cmd)
    ran_at = now()
    q_response = mdb.command(query.cmd.dict(exclude_unset=True))
    cmd_response: CommandResponse = command_response_for(q_type)(**q_response)
    query_run = (
        QueryRun(qid=query.id, ran_at=ran_at, result=cmd_response)
        if cmd_response.ok
        else QueryRun(qid=query.id, ran_at=ran_at, error=cmd_response)
    )
    mdb.query_runs.insert_one(query_run.dict(exclude_unset=True))
    return cmd_response
