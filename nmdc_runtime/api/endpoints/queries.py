import json
from typing import List

import bson.json_util
from fastapi import APIRouter, Depends, status, HTTPException
from pymongo.database import Database as MongoDatabase

from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.core.util import now, raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import permitted, users_allowed
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
    if not permitted(user.username, "/queries:run(query_cmd:DeleteCommand)"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"only users {users_allowed('/queries:run(query_cmd:DeleteCommand)')} "
                "are allowed to issue delete commands.",
            ),
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
    mdb.queries.insert_one(query.model_dump(exclude_unset=True))
    cmd_response = _run_query(query, mdb)
    return unmongo(cmd_response.model_dump(exclude_unset=True))


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
    return unmongo(cmd_response.model_dump(exclude_unset=True))


def _run_query(query, mdb) -> CommandResponse:
    q_type = type(query.cmd)
    ran_at = now()
    if q_type is DeleteCommand:
        collection_name = query.cmd.delete
        find_specs = [
            {"filter": dcd.q, "limit": dcd.limit} for dcd in query.cmd.deletes
        ]
        for spec in find_specs:
            docs = list(mdb[collection_name].find(**spec))
            if not docs:
                continue
            insert_many_result = mdb.client["nmdc_deleted"][
                collection_name
            ].insert_many({"doc": d, "deleted_at": ran_at} for d in docs)
            if len(insert_many_result.inserted_ids) != len(docs):
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to back up to-be-deleted documents. operation aborted.",
                )

    q_response = mdb.command(query.cmd.model_dump(exclude_unset=True))
    cmd_response: CommandResponse = command_response_for(q_type)(**q_response)
    query_run = (
        QueryRun(qid=query.id, ran_at=ran_at, result=cmd_response)
        if cmd_response.ok
        else QueryRun(qid=query.id, ran_at=ran_at, error=cmd_response)
    )
    mdb.query_runs.insert_one(query_run.model_dump(exclude_unset=True))
    return cmd_response
