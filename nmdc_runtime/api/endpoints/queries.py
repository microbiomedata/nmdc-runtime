import json
import logging

import bson.json_util
from fastapi import APIRouter, Depends, status, HTTPException
from pymongo.database import Database as MongoDatabase
from toolz import assoc_in, dissoc

from nmdc_runtime.api.core.util import now
from nmdc_runtime.api.db.mongo import (
    get_mongo_db,
    get_nonempty_nmdc_schema_collection_names,
)
from nmdc_runtime.api.endpoints.util import (
    check_action_permitted,
    strip_oid,
)
import nmdc_runtime.api.models.query_continuation as qc
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
    
def check_can_aggregate(user: User):
    # aggregate queries require same level of permissions
    if not check_action_permitted(
        user.username, "/queries:run(query_cmd:AggregateCommand)"
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only specific users are allowed to issue aggregate commands.",
        )

# Note: We set `response_model_exclude_unset=True` so that all the properties of the `CommandResponseOptions` object
#       that we don't explicitly assign values to while handling the HTTP request, are omitted from the HTTP response.
#       Reference: https://fastapi.tiangolo.com/tutorial/response-model/#use-the-response_model_exclude_unset-parameter
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
    Performs `find`, `aggregate`, `update`, `delete`, and `getMore` commands for users that have adequate permissions.

    For `find` and `aggregate` commands, the requested items will be in `cursor.batch`.
    When the response includes a non-null `cursor.id`, there _may_ be more items available.
    To retrieve the next batch of items, submit a request with `getMore` set to that non-null `cursor.id`.
    When the response includes a null `cursor.id`, there are no more items available.

    **Example request bodies:**

    Get all\* biosamples.
    ```
    {
      "find": "biosample_set",
      "filter": {}
    }
    ```

    Get all\* biosamples associated with a given study.
    ```
    {
      "find": "biosample_set",
      "filter": {"associated_studies": "nmdc:sty-11-34xj1150"}
    }
    ```

    \*<small>Up to 101, which is the default "batchSize" for the "find" command.</small>

    Get the first 200 biosamples associated with a given study.
    ```
    {
      "find": "biosample_set",
      "filter": {"associated_studies": "nmdc:sty-11-34xj1150"},
      "batchSize": 200
    }
    ```

    Delete the first biosample having a given `id`.
    ```
    {
      "delete": "biosample_set",
      "deletes": [{"q": {"id": "A_BIOSAMPLE_ID"}, "limit": 1}]
    }
    ```

    Rename all biosamples having a given `id`.
    ```
    {
      "update": "biosample_set",
      "updates": [{"q": {"id": "A_BIOSAMPLE_ID"}, "u": {"$set": {"name": "A_NEW_NAME"}}}]
    }
    ```

    Get all\* biosamples, sorted by the number of studies associated with them (greatest to least).
    ```
    {
      "aggregate": "biosample_set",
      "pipeline": [{"$sortByCount": "$associated_studies"}]
    }
    ```

    \*<small>Up to 25, which is the default "batchSize" for the "aggregate" command.</small>

    Get the first 10 biosamples having the largest numbers of studies associated with them,
    sorted by that number of studies (greatest to least).
    ```
    {
      "aggregate": "biosample_set",
      "pipeline": [{"$sortByCount": "$associated_studies"}],
      "cursor": {"batchSize": 10}
    }
    ```

    Use the `cursor.id` from a previous response to get the next batch of results,
    whether that batch is empty or non-empty.
    ```
    {
      "getMore": "somecursorid"
    }
    ```

    **Limitations:**

    1. The maximum size of the response payload is 16 MB. You can use the "batchSize" property
       (for "find" commands) or the "cursor.batchSize" property (for "aggregate" commands)—along
       with some trial and error—to ensure the response payload size remains under that limit.
    2. When using an "aggregate" command, if any of the objects output by the pipeline lacks an
       "_id" field, the endpoint will return only the first batch of objects and will not offer
       pagination (i.e. "cursor.id" will be null).
    3. Manipulating the values of "_id" fields within an aggregation pipeline (e.g. via "$set")
       can result in pagination not working. If this impacts your use case, please contact us.
    """
    r"""
    Additional notes for developers:*
    --------------------------------
    * Note: Because this section isn't in the main docstring,
            it isn't visible on Swagger UI.

    1. The sorting part of the pagination algorithm is based upon the assumption
       that the `_id` values of the documents are "deterministically sortable."
       However, an aggregation pipeline can be used to populate the `_id` field
       with values that are _not_ "deterministically sortable." For example,
       the final stage of the pipeline could be: `{ "$set": { "_id": "potato" } }`
       References: 
       - https://www.unicode.org/notes/tn9/
       - https://www.mongodb.com/docs/manual/reference/operator/aggregation/set/
    """

    # If the command is one that requires the user to have specific permissions, check for those permissions now.
    # Note: The permission-checking function will raise an exception if the user lacks those permissions.
    if isinstance(cmd, (DeleteCommand, UpdateCommand)):
        check_can_update_and_delete(user)

    # check if the user has permission to run aggregate commands
    if isinstance(cmd, AggregateCommand):
        check_can_aggregate(user)

    cmd_response = _run_mdb_cmd(cmd)
    return cmd_response


_mdb = get_mongo_db()


def _run_mdb_cmd(cmd: Cmd, mdb: MongoDatabase = _mdb) -> CommandResponse:
    r"""
    TODO: Document this function.
    TODO: Consider splitting this function into multiple, smaller functions (if practical). It is currently ~220 lines.
    TODO: How does this function behave when the "batchSize" is invalid (e.g. 0, negative, non-numeric)?
    """
    ran_at = now()
    cursor_id = cmd.getMore if isinstance(cmd, GetMoreCommand) else None
    logging.info(f"Command type: {type(cmd).__name__}")
    logging.info(f"Cursor ID: {cursor_id}")

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
        query_continuation = qc.get_qc_by__id(cursor_id)
        # construct "getMore" equivalent of originating "find" or "aggregate" query.
        initial_cmd_doc: dict = qc.get_initial_query_for_qc(
            query_continuation
        ).model_dump(exclude_unset=True)
        if "find" in initial_cmd_doc:
            modified_cmd_doc = assoc_in(
                initial_cmd_doc,
                ["filter", "_id", "$gt"],
                qc.get_last_doc__id_for_qc(query_continuation),
            )
            cmd = FindCommand(**modified_cmd_doc)
        elif "aggregate" in initial_cmd_doc:
            initial_cmd_doc["pipeline"].append(
                {
                    "$match": {
                        "_id": {"$gt": qc.get_last_doc__id_for_qc(query_continuation)}
                    }
                }
            )
            modified_cmd_doc = assoc_in(
                initial_cmd_doc,
                ["pipeline"],
                initial_cmd_doc["pipeline"]
                + [
                    {
                        "$match": {
                            "_id": {
                                "$gt": qc.get_last_doc__id_for_qc(query_continuation)
                            }
                        }
                    }
                ],
            )

            cmd = AggregateCommand(**modified_cmd_doc)
        else:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="The specified 'getMore' value resolved to an invalid command.",
            )

    # Issue `cmd` (possibly modified) as a mongo command, and ensure a well-formed response.
    #  transform e.g. `{"$oid": "..."}` instances in model_dump to `ObjectId("...")` instances.
    logging.info(
        f"Command JSON: {bson.json_util.loads(json.dumps(cmd.model_dump(exclude_unset=True)))}"
    )

    # Send a command to the database and get the raw response. If the command was a
    # cursor-yielding command, make a new response object in which the raw response's
    # `cursor.firstBatch`/`cursor.nextBatch` value is in a field named `cursor.batch`.
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
        del cmd_response_adapted["cursor"][batch_key]
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
    query_continuation = None

    # TODO: Handle empty cursor response or situations where batch < batchSize.
    #
    #       Note: This "TODO" comment has not been removed, but — based upon the
    #             results of the automated tests, which do submit "find" and
    #             "aggregation" commands that produce empty result sets and result
    #             sets smaller than one batch — I think this has been resolved.
    #
    if isinstance(cmd, CursorYieldingCommand) and cmd_response.cursor.id == "0":
        # No cursor id returned. No need to create a continuation.
        cmd_response.cursor.id = None
        return cmd_response

    # Cursor id returned. Create a continuation.
    if isinstance(cmd, AggregateCommand):
        slimmed_command_response = CursorYieldingCommandResponse.slimmed(cmd_response)

        # First, we check whether the "slimmed" command response is `None`. That can only happen
        # when some of the documents in the batch lack an `_id` field. We do not support pagination
        # in that scenario (since our pagination algorithm relies on the `_id` values).
        if slimmed_command_response is None:
            logging.warning(
                "Failed to obtain list of `_id` values. Will return batch and no pagination token."
            )
            cmd_response.cursor.id = None  # explicitly set the pagination token to null
            return cmd_response

        query_continuation = qc.create_qc(cmd, slimmed_command_response)
        cmd_response.cursor.id = (
            None if cmd_response.cursor.id == "0" else query_continuation.id
        )
    elif isinstance(cmd, FindCommand):
        query_continuation = qc.create_qc(
            cmd, CursorYieldingCommandResponse.slimmed(cmd_response)
        )
        cmd_response.cursor.id = (
            None if cmd_response.cursor.id == "0" else query_continuation.id
        )
    elif isinstance(cmd, GetMoreCommand):
        # Append query run to current continuation
        query_continuation = qc.get_qc_by__id(cursor_id)
        query_continuation.cmd_responses.append(
            CursorYieldingCommandResponse.cursor_batch__ids_only(cmd_response)
        )
        cmd_response.cursor.id = (
            None if cmd_response.cursor.id == "0" else query_continuation.id
        )

    return cmd_response
