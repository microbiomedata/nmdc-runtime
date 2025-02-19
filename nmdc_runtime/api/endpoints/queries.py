import json
from typing import List

import bson.json_util
from fastapi import APIRouter, Depends, status, HTTPException
from pymongo.database import Database as MongoDatabase
from toolz import assoc_in, dissoc

from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.core.util import now, raise404_if_none, pick
from nmdc_runtime.api.db.mongo import (
    get_mongo_db,
    get_nonempty_nmdc_schema_collection_names,
)
from nmdc_runtime.api.endpoints.util import (
    check_action_permitted,
    strip_oid,
)
import nmdc_runtime.api.models.query_continuation as cc
from nmdc_runtime.api.models.query import (
    DeleteCommand,
    CommandResponse,
    command_response_for,
    QueryCmd,
    UpdateCommand,
    AggregateCommand,
    FindCommand,
    GetMoreCommand,
    CommandResponseOptions,
    Cmd,
    CursorYieldingCommandResponse,
    CursorYieldingCommand,
)
from nmdc_runtime.api.models.user import get_current_active_user, User
from nmdc_runtime.util import OverlayDB, validate_json

router = APIRouter()


def check_can_update_and_delete(user: User):
    # update and delete queries require same level of permissions
    if not check_action_permitted(
        user.username, "/queries:run(query_cmd:DeleteCommand)"
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only specific users are allowed to issue update and delete commands.",
        )


@router.post(
    "/queries:run",
    response_model=CommandResponseOptions,
    response_model_exclude_unset=True,
)
def run_query(
    cmd: Cmd,
    mdb: MongoDatabase = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    """
    Allows `find`, `aggregate`, `update`, `delete`, etc. commands for users with permissions.

    For `find` and `aggregate`, a `cursor` `id` may be returned, which can be used with a `getMore` command
    to retrieve the next `batch` of documents. Note that the maximum size of the response payload is 16MB, so
    you may need to set the `batchSize` parameter appropriately for `find` or `aggregate`.

    Examples:
    ```
    {
      "find": "biosample_set",
      "filter": {}
    }

    {
      "find": "biosample_set",
      "filter": {"associated_studies": "nmdc:sty-11-34xj1150"}
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
        "pipeline": [{"$sortByCount": "$associated_studies"}],
        "cursor": {"batchSize": 25}
    }
    ```
    """
    if isinstance(cmd, (DeleteCommand, UpdateCommand)):
        check_can_update_and_delete(user)
    cmd_response = _run_mdb_cmd(cmd)
    return cmd_response


_mdb = get_mongo_db()


def _run_mdb_cmd(cmd: Cmd, mdb: MongoDatabase = _mdb) -> CommandResponse:
    ran_at = now()
    cursor_id = cmd.getMore if isinstance(cmd, GetMoreCommand) else None

    if isinstance(cmd, DeleteCommand):
        collection_name = cmd.delete
        if collection_name not in get_nonempty_nmdc_schema_collection_names(mdb):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Can only delete documents in nmdc-schema collections.",
            )
        delete_specs = [
            {"filter": del_statement.q, "limit": del_statement.limit}
            for del_statement in cmd.deletes
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
    elif isinstance(cmd, UpdateCommand):
        collection_name = cmd.update
        if collection_name not in get_nonempty_nmdc_schema_collection_names(mdb):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Can only update documents in nmdc-schema collections.",
            )
        update_specs = [
            {"filter": up_statement.q, "limit": 0 if up_statement.multi else 1}
            for up_statement in cmd.updates
        ]
        # Execute this "update" command on a temporary "overlay" database so we can
        # validate its outcome before executing it on the real database. If its outcome
        # is invalid, we will abort and raise an "HTTP 422" exception.
        #
        # TODO: Consider wrapping this entire "preview-then-apply" sequence within a
        #       MongoDB transaction so as to avoid race conditions where the overlay
        #       database at "preview" time does not reflect the state of the database
        #       at "apply" time. This will be necessary once the "preview" step
        #       accounts for referential integrity.
        #
        with OverlayDB(mdb) as odb:
            odb.apply_updates(
                collection_name,
                [u.model_dump(mode="json", exclude="hint") for u in cmd.updates],
            )
            _ids_to_check = set()
            for spec in update_specs:
                for doc in mdb[collection_name].find(
                    filter=spec["filter"],
                    limit=spec["limit"],
                    projection={
                        "_id": 1
                    },  # unique `id` not guaranteed (see e.g. `functional_annotation_agg`)
                ):
                    _ids_to_check.add(doc["_id"])
            docs_to_check = odb._top_db[collection_name].find(
                {"_id": {"$in": list(_ids_to_check)}}
            )
            rv = validate_json(
                {collection_name: [strip_oid(d) for d in docs_to_check]}, mdb
            )
            if rv["result"] == "errors":
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Schema document(s) would be invalid after proposed update: {rv['detail']}",
                )
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
    elif isinstance(cmd, AggregateCommand):
        # Append $sort stage to pipeline and allow disk use.
        cmd.pipeline.append({"$sort": {"_id": 1}})
        cmd.allowDiskUse = True
    elif isinstance(cmd, FindCommand):
        cmd.sort = (cmd.sort or {}) | {"_id": 1}
    elif isinstance(cmd, GetMoreCommand):
        # Fetch query continuation for query, construct "getMore" equivalent, and assign `query` to that equivalent.
        cursor_continuation = cc.get_cc_by_id(cursor_id)
        # construct "getMore" equivalent of originating "find" or "aggregate" query.
        initial_cmd_doc: dict = cc.initial_query_for_cc(cursor_continuation).model_dump(
            exclude_unset=True
        )["cmd"]
        if "find" in initial_cmd_doc:
            modified_cmd_doc = assoc_in(
                initial_cmd_doc,
                ["filter", "_id", "$gt"],
                cc.last_doc__id_for_cc(cursor_continuation),
            )
            query_cmd = FindCommand(**modified_cmd_doc)
        elif "aggregate" in initial_cmd_doc:
            # TODO assign `query` cmd to equivalent aggregate cmd.
            pass

    # Issue `cmd` (possibly modified) as a mongo command, and ensure a well-formed response.
    #  transform e.g. `{"$oid": "..."}` instances in model_dump to `ObjectId("...")` instances.
    cmd_response_raw: dict = mdb.command(
        bson.json_util.loads(json.dumps(cmd.model_dump(exclude_unset=True)))
    )
    if isinstance(cmd, CursorYieldingCommand):
        batch_key = "firstBatch" if isinstance(cmd, QueryCmd) else "nextBatch"
        cmd_response_adapted = assoc_in(
            cmd_response_raw,
            ["cursor", "batch"],
            cmd_response_raw["cursor"][batch_key],
        )
        del cmd_response_raw["cursor"][batch_key]
    else:
        cmd_response_adapted = cmd_response_raw

    cmd_response: CommandResponse = command_response_for(type(cmd))(
        **cmd_response_adapted
    )

    # Not okay? Early return.
    if not cmd_response.ok:
        return cmd_response

    if isinstance(cmd, (DeleteCommand, UpdateCommand)):
        # TODO `_request_dagster_run` of `ensure_alldocs`?
        if cmd_response.n == 0:
            raise HTTPException(
                status_code=status.HTTP_418_IM_A_TEAPOT,
                detail=(
                    f"{'update' if isinstance(cmd, UpdateCommand) else 'delete'} command modified zero documents."
                    " I'm guessing that's not what you expected. Check the syntax of your request."
                    " But what do I know? I'm just a teapot.",
                ),
            )

    # Cursor-command response? Prep runtime-managed cursor id and replace mongo session cursor id in response.
    cursor_continuation = None
    if isinstance(cmd, AggregateCommand):
        # TODO
        cursor_continuation = cc.create_cc(cmd_response)
    elif isinstance(cmd, FindCommand):
        cursor_continuation = cc.create_cc(
            cmd, CursorYieldingCommandResponse.slimmed(cmd_response)
        )
        cmd_response.cursor.id = (
            None if cmd_response.cursor.id == "0" else cursor_continuation.id
        )
    elif isinstance(cmd, GetMoreCommand):
        # Append query run to current continuation
        cursor_continuation = cc.get_cc_by_id(cursor_id)
        cursor_continuation.cmd_responses.append(
            CursorYieldingCommandResponse.cursor_batch__ids_only(cmd_response)
        )
        cmd_response.cursor.id = (
            None if cmd_response.cursor.id == "0" else cursor_continuation.id
        )
        print(f"getmore:{cmd_response.cursor.id=}")
    return cmd_response
