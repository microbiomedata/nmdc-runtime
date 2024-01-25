import json
from typing import List

import bson.json_util
from fastapi import APIRouter, Depends, status, HTTPException
from pymongo.database import Database as MongoDatabase

from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.core.util import now, raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db, nmdc_schema_collection_names
from nmdc_runtime.api.endpoints.util import permitted, users_allowed
from nmdc_runtime.api.models.query import (
    Query,
    QueryResponseOptions,
    DeleteCommand,
    QueryRun,
    CommandResponse,
    command_response_for,
    QueryCmd,
    UpdateCommand,
)
from nmdc_runtime.api.models.user import get_current_active_user, User

router = APIRouter()


def unmongo(d: dict) -> dict:
    """Ensure a dict with e.g. mongo ObjectIds will serialize as JSON."""
    return json.loads(bson.json_util.dumps(d))


def check_can_update_and_delete(user: User):
    # update and delete queries require same level of permissions
    if not permitted(user.username, "/queries:run(query_cmd:DeleteCommand)"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only specific users are allowed to issue update and delete commands.",
        )


@router.post("/queries:run", response_model=QueryResponseOptions)
def run_query(
    query_cmd: QueryCmd,
    mdb: MongoDatabase = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    """
    Allows `find`, `aggregate`, `update`, and `delete` commands for users with permissions.

    For `find` and `aggregate`, note that cursor batching/pagination does *not*
    work via this API, so ensure that you construct a command that will return
    what you need in the "first batch". Also, the maximum size of the returned payload is 16MB.

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

    {
        "update": "biosample_set",
        "updates": [{"q": {"id": "YOUR_BIOSAMPLE_ID"}, "u": {"$set": {"name": "A_NEW_NAME"}}}]
    }

    {
        "aggregate": "biosample_set",
        "pipeline": [{"$sortByCount": "$part_of"}],
        "cursor": {"batchSize": 25}
    }
    ```
    """
    if isinstance(query_cmd, (DeleteCommand, UpdateCommand)):
        check_can_update_and_delete(user)

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
        check_can_update_and_delete(user)

    cmd_response = _run_query(query, mdb)
    return unmongo(cmd_response.model_dump(exclude_unset=True))


def _run_query(query, mdb) -> CommandResponse:
    q_type = type(query.cmd)
    ran_at = now()
    if q_type is DeleteCommand:
        collection_name = query.cmd.delete
        if collection_name not in nmdc_schema_collection_names(mdb):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Can only delete documents in nmdc-schema collections.",
            )
        delete_specs = [
            {"filter": del_statement.q, "limit": del_statement.limit}
            for del_statement in query.cmd.deletes
        ]
        for spec in delete_specs:
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
    elif q_type is UpdateCommand:
        collection_name = query.cmd.update
        if collection_name not in nmdc_schema_collection_names(mdb):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Can only update documents in nmdc-schema collections.",
            )
        update_specs = [
            {"filter": up_statement.q, "limit": 0 if up_statement.multi else 1}
            for up_statement in query.cmd.updates
        ]
        for spec in update_specs:
            docs = list(mdb[collection_name].find(**spec))
            if not docs:
                continue
            insert_many_result = mdb.client["nmdc_updated"][
                collection_name
            ].insert_many({"doc": d, "updated_at": ran_at} for d in docs)
            if len(insert_many_result.inserted_ids) != len(docs):
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to back up to-be-updated documents. operation aborted.",
                )

    q_response = mdb.command(query.cmd.model_dump(exclude_unset=True))
    cmd_response: CommandResponse = command_response_for(q_type)(**q_response)
    query_run = (
        QueryRun(qid=query.id, ran_at=ran_at, result=cmd_response)
        if cmd_response.ok
        else QueryRun(qid=query.id, ran_at=ran_at, error=cmd_response)
    )
    if q_type in (DeleteCommand, UpdateCommand) and cmd_response.n == 0:
        raise HTTPException(
            status_code=status.HTTP_418_IM_A_TEAPOT,
            detail=(
                f"{'update' if q_type is UpdateCommand else 'delete'} command modified zero documents."
                " I'm guessing that's not what you expected. Check the syntax of your request."
                " But what do I know? I'm just a teapot.",
            ),
        )
    mdb.query_runs.insert_one(query_run.model_dump(exclude_unset=True))
    return cmd_response
