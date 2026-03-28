import logging
from typing import Any, Dict, List, Set, Annotated, Tuple

import pymongo
from bson import ObjectId
from fastapi import APIRouter, Body, Depends, HTTPException, Path, status
from pymongo.database import Database as MongoDatabase
from pymongo.operations import InsertOne
from pymongo.errors import BulkWriteError
from pymongo.results import ClientBulkWriteResult

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.endpoints.lib.workflow_executions import (
    derive_predecessor_id,
    derive_successor_id,
    make_pattern_matching_ids_having_base_id_in_list,
    parse_workflow_execution_id,
    prepare_supersession_chain_for_workflow_execution_deletion,
    update_superseded_by_field_of_data_objects_having_id_in_list,
)
from nmdc_runtime.api.endpoints.queries import (
    _run_mdb_cmd,
    check_can_update_and_delete,
    _run_delete_nonschema,
)
from nmdc_runtime.api.db.mongo import get_mongo_db, validate_json
from nmdc_runtime.api.endpoints.util import check_action_permitted
from nmdc_runtime.api.models.allowance import AllowanceAction
from nmdc_runtime.api.models.capability import Capability
from nmdc_runtime.api.models.object_type import ObjectType
from nmdc_runtime.api.models.query import DeleteCommand, DeleteStatement
from nmdc_runtime.api.models.site import Site, get_current_client_site
from nmdc_runtime.api.models.user import User, get_current_active_user
from nmdc_runtime.api.models.util import DeleteResponse
from nmdc_runtime.api.models.workflow import Workflow
from nmdc_schema.nmdc import (
    MetagenomeAnnotation,
    MetaproteomicsAnalysis,
    MetatranscriptomeAnnotation,
)

from nmdc_runtime.util import duration_logger


router = APIRouter()


@router.get("/workflows", response_model=List[Workflow])
def list_workflows(
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return list(mdb.workflows.find())


@router.get("/workflows/{workflow_id}", response_model=Workflow)
def get_workflow(
    workflow_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return raise404_if_none(mdb.workflows.find_one({"id": workflow_id}))


@router.get("/workflows/{workflow_id}/object_types", response_model=List[ObjectType])
def list_workflow_object_types(
    workflow_id: str, mdb: pymongo.database.Database = Depends(get_mongo_db)
):
    object_type_ids = [
        doc["object_type_id"] for doc in mdb.triggers.find({"workflow_id": workflow_id})
    ]
    return list(mdb.object_types.find({"id": {"$in": object_type_ids}}))


@router.get("/workflows/{workflow_id}/capabilities", response_model=List[Capability])
def list_workflow_capabilities(
    workflow_id: str, mdb: pymongo.database.Database = Depends(get_mongo_db)
):
    doc = raise404_if_none(mdb.workflows.find_one({"id": workflow_id}))
    return list(mdb.capabilities.find({"id": {"$in": doc.get("capability_ids", [])}}))


@router.post("/workflows/activities", status_code=status.HTTP_410_GONE, deprecated=True)
async def post_activity(
    activity_set: dict[str, Any],
    site: Site = Depends(get_current_client_site),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    DEPRECATED: migrate all workflows from this endpoint to `/workflows/workflow_executions`.
    """
    return f"DEPRECATED: POST your request to `/workflows/workflow_executions` instead."


@router.post(
    "/workflows/workflow_executions",
    description=(
        "Create workflow executions and related metadata, by submitting an "
        "`nmdc:Database` instance that includes a `workflow_execution_set` collection."
    ),
)
async def post_workflow_execution(
    database_in: Annotated[
        dict[str, Any],
        Body(
            title="nmdc:Database instance",
            description=(
                "An `nmdc:Database` instance that includes at least "
                "a `workflow_execution_set` collection and, optionally, "
                "other collections consisting of documents related to "
                "the included `workflow_execution_set` documents."
            ),
            examples=[
                {
                    "workflow_execution_set": [],
                }
            ],
        ),
    ],
    user: User = Depends(get_current_active_user),
    mdb: MongoDatabase = Depends(get_mongo_db),
) -> Dict[str, str]:
    """
    Create workflow executions and related metadata.

    TODO: Warning! This endpoint can currently be used to submit _all_ types of metadata! Since we
          currently rely on validation at the application level, as opposed to validation at the
          database level, keep this endpoint in mind when introducing new validation processes.

    High-level algorithm:

       Terminology:
       - "co-submitted" means "submitted together in the same payload to this endpoint"
       - "existing" means "already in the Mongo database"
       - "WFE" is short for `WorkflowExecution`
       - "DOBJ" is short for `DataObject`

    1. For each submitted WFE, determine whether its `id` suffix indicates that it is SUPERSEDED BY
       another WFE, whether the latter is co-submitted or existing. If it does, do two things:
       (a) update the `superseded_by` field of that submitted WFE; and
       (b) update the `superseded_by` fields of its output DOBJs, if any,
           whether co-submitted or existing.
    2. For each submitted WFE, determine whether its `id` indicates that it SUPERSEDES an existing
       WFE (we don't bother checking whether it supersedes a co-submitted WFE, since we would have
       handled that in step 1). If it does, do two things:
       (a) update the `superseded_by` field of that existing WFE; and
       (b) update the `superseded_by` fields of that existing WFE's output DOBJs, if any,
           whether co-submitted or existing.
    3. Proceed to do the insertion of the [maybe manipulated] submission payload.
       Perform all of the above updates and insertions within a single transaction,
       so we can roll back if something goes wrong.

    Reference: https://microbiomedata.github.io/nmdc-schema/superseded_by/

    Note: There is a race condition where a _referenced_ document could be deleted from the database
          between the time the ref. int. check is performed via `validate_json` and the time the
          transaction is started and the referring document is actually inserted into the database.
          This is a general issue for all endpoints that rely on application-level validation
          outside of a Mongo transaction, since we have no DBMS-level validation in place.
    """

    # TODO: Decouple this endpoint's authorization criteria from that of the `/metadata/json:submit`
    #       endpoint. For now, we use the same criteria, since the core functionality of both
    #       endpoints are so similar to one another (i.e. insert metadata as an `nmdc:Database`).
    if not check_action_permitted(user.username, AllowanceAction.SUBMIT_JSON.value):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only specific users can submit workflow executions.",
        )

    id_only_projection = {"id": 1, "_id": 0}  # alias for common Mongo projection

    # Get references to relevant MongoDB collections.
    workflow_execution_set = mdb.get_collection("workflow_execution_set")
    data_object_set = mdb.get_collection("data_object_set")

    # Initialize a list of the `id`s of the "existing" (i.e. already in the database) WFEs
    # whose `id`s share the same `base_id` as any of the submitted WFEs. According to the suffix
    # convention established by the NMDC workflow automation team members, such WFEs are on the
    # same supersession chain as one another.
    relevant_existing_wfe_ids: List[str] = []

    with duration_logger(logging.info, "Performing preliminary validation"):
        # If the payload has a top-level key named "@type" (which the `validate_json` function
        # considers to be valid), strip it away now.
        if "@type" in database_in:
            database_in.pop("@type")

        # Do some preliminary validation so our subsequent supersession chain management code can
        # take for granted the fact that the payload constitutes a valid `nmdc:Database` instance.
        validation_result = validate_json(
            database_in,
            mdb,
            check_inter_document_references=True,
        )
        if (
            validation_result["result"] == "errors"
            or "workflow_execution_set" not in database_in
        ):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Request payload must represent a valid 'nmdc:Database' instance "
                    "that includes a 'workflow_execution_set' collection. "
                    f"Validation result: {str(validation_result)}"
                ),
            )

        # Make lists that will be useful to subsequent code.
        submitted_wfes: List[dict] = database_in["workflow_execution_set"]
        submitted_wfe_ids: List[str] = [wfe["id"] for wfe in submitted_wfes]
        submitted_dobjs: List[dict] = database_in.get("data_object_set", [])

    # Start a Mongo transaction before we write anything to the database.
    with duration_logger(logging.info, "Using MongoDB transaction"):
        with mdb.client.start_session() as session:
            with session.start_transaction():
                with duration_logger(
                    logging.info,
                    "Gathering WFE IDs relevant to supersession management",
                ):
                    # Extract the `base_id` and `run_number` from each submitted
                    # `WorkflowExecution`'s `id`; raising an error if any of them lacks
                    # a `run_number` (since our workflow automation system currently relies upon
                    # run numbers, despite the schema currently saying they are optional and the
                    # minter currently minting `id`s that lack them).
                    id_parts_by_submitted_wfe_id: Dict[str, Tuple[str, int | None]] = {}
                    for submitted_wfe_id in submitted_wfe_ids:
                        base_id, run_number = parse_workflow_execution_id(
                            submitted_wfe_id
                        )
                        if run_number is None:
                            raise HTTPException(
                                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=(
                                    f"The ID of WorkflowExecution '{submitted_wfe_id}' is not in the "
                                    "format required by our supersession management processes."
                                ),
                            )
                        id_parts_by_submitted_wfe_id[submitted_wfe_id] = (
                            base_id,
                            run_number,
                        )

                    # Now that we have all the submitted `base_id` values, get the `id` of every
                    # `WorkflowExecution` in the database whose `id` shares any of those `base_id`
                    # values. These are our potential anteceding (coming before) and superseding
                    # (coming after) WFEs.
                    if len(id_parts_by_submitted_wfe_id.keys()) > 0:
                        base_ids: List[str] = []
                        for _, (base_id, _) in id_parts_by_submitted_wfe_id.items():
                            if base_id not in base_ids:  # prevents duplicates
                                base_ids.append(base_id)
                        pattern = make_pattern_matching_ids_having_base_id_in_list(
                            base_ids
                        )
                        filter_ = {"id": {"$regex": pattern}}
                        relevant_existing_wfes_cursor = workflow_execution_set.find(
                            filter=filter_,
                            projection=id_only_projection,
                            session=session,
                        )
                        relevant_existing_wfe_ids = [
                            wfe["id"] for wfe in relevant_existing_wfes_cursor
                        ]
                    logging.info(
                        f"Found {len(relevant_existing_wfe_ids)} relevant existing WFEs"
                    )

                with duration_logger(
                    logging.info, "Preparing to insert superseded WFEs and DOBJs"
                ):
                    # For each submitted `WorkflowExecution`, check whether it is superseded by any
                    # other `WorkflowExecution` (whether a co-submitted one or one already in the
                    # database) according to the `id`-derivable supersession relationships.
                    #
                    # If it is, update the `superseded_by` field of that submitted `WorkflowExecution`
                    # in the insertion payload; and update the `superseded_by` fields of its output
                    # `DataObject`s, if any, whether those are in the insertion payload or already
                    # in the database.
                    #
                    submitted_and_relevant_existing_wfe_ids = (
                        submitted_wfe_ids + relevant_existing_wfe_ids
                    )
                    for submitted_wfe_id in submitted_wfe_ids:
                        successor_wfe_id = derive_successor_id(submitted_wfe_id)
                        if successor_wfe_id in submitted_and_relevant_existing_wfe_ids:
                            logging.info(
                                f"WorkflowExecution '{submitted_wfe_id}' is superseded by "
                                f"co-submitted or existing WorkflowExecution '{successor_wfe_id}'."
                            )
                            # Update the insertion payload and database accordingly.
                            for submitted_wfe in submitted_wfes:
                                if submitted_wfe["id"] == submitted_wfe_id:
                                    submitted_wfe["superseded_by"] = successor_wfe_id
                                    update_superseded_by_field_of_data_objects_having_id_in_list(
                                        data_object_ids=submitted_wfe.get(
                                            "has_output", []
                                        ),
                                        data_object_list=submitted_dobjs,
                                        data_object_set_collection=data_object_set,
                                        superseded_by=successor_wfe_id,
                                        client_session=session,
                                    )
                                    # Since we found the `WorkflowExecution` we were looking for,
                                    # we can stop looking for it.
                                    break
                        else:
                            logging.debug(
                                f"WorkflowExecution '{submitted_wfe_id}' is not superseded by "
                                "any co-submitted or existing WorkflowExecution."
                            )

                with duration_logger(
                    logging.info, "Preparing to insert superseding WFEs"
                ):
                    # For each submitted `WorkflowExecution`, check whether it supersedes any existing
                    # `WorkflowExecution` in the database (we don't bother checking the co-submitted
                    # ones because we would have already handled those in the previous step).
                    #
                    # If it does, update the `superseded_by` fields of those existing
                    # `WorkflowExecution`s; and of their output `DataObject`s, if any, whether those
                    # are in the insertion payload or already in the database.
                    #
                    for submitted_wfe_id in submitted_wfe_ids:
                        predecessor_wfe_id = derive_predecessor_id(submitted_wfe_id)
                        if predecessor_wfe_id in relevant_existing_wfe_ids:
                            logging.info(
                                f"WorkflowExecution '{submitted_wfe_id}' supersedes "
                                f"existing WorkflowExecution '{predecessor_wfe_id}'."
                            )
                            wfe_document = workflow_execution_set.find_one(
                                {"id": predecessor_wfe_id},
                                {"has_output": 1, "_id": 0},
                                session=session,
                            )
                            if wfe_document is not None:
                                workflow_execution_set.update_one(
                                    {"id": predecessor_wfe_id},
                                    {"$set": {"superseded_by": submitted_wfe_id}},
                                    session=session,
                                )
                                update_superseded_by_field_of_data_objects_having_id_in_list(
                                    data_object_ids=wfe_document.get("has_output", []),
                                    data_object_list=submitted_dobjs,
                                    data_object_set_collection=data_object_set,
                                    superseded_by=submitted_wfe_id,
                                    client_session=session,
                                )

                # Generate pymongo operation instances that account for the insertion operations
                # for the submitted data (with any "patches" introduced above intact).
                #
                # Note: The updates of the existing data will already have been performed
                #       above (as part of the ongoing Mongo transaction).
                #
                # Note: Beginning with pymongo 4.9 (and MongoDB 8.0), we can perform bulk writes
                #       across collections (notice the "namespace" parameter used below).
                #
                # Docs:
                # - https://pymongo.readthedocs.io/en/4.16.0/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient.bulk_write
                # - https://pymongo.readthedocs.io/en/4.16.0/api/pymongo/operations.html
                #
                ops = []
                for collection_name, documents in database_in.items():
                    namespace = f"{mdb.name}.{collection_name}"  # e.g. "nmdc.workflow_execution_set"
                    for document in documents:
                        op = InsertOne(document, namespace=namespace)
                        ops.append(op)

                try:
                    # Perform the operations via the `bulk_write` function (which is supposedly
                    # faster that a sequence of `insert_one` invocations).
                    if len(ops) > 0:
                        bulk_write_result: ClientBulkWriteResult = (
                            mdb.client.bulk_write(
                                ops,
                                bypass_document_validation=True,
                                session=session,
                                comment="Bulk insertion via 'POST /workflows/workflow_executions' endpoint",
                            )
                        )
                        num_inserted = bulk_write_result.inserted_count
                        logging.info(
                            f"Inserted {num_inserted} documents via a bulk_write."
                        )
                        return {"message": f"Inserted {num_inserted} documents"}
                    return {"message": "Done"}
                except BulkWriteError as e:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT, detail=str(e)
                    )


@router.delete(
    "/workflows/workflow_executions/{workflow_execution_id}",
    response_model=DeleteResponse,
    description="Delete a workflow execution and its downstream workflow executions, data objects, "
    "functional annotation aggregation members, and related job records.\n\n"
    "This endpoint performs recursive deletion of the specified workflow execution, "
    "all downstream workflow executions that depend on this workflow execution's outputs, "
    "all functional annotation aggregation members generated by deleted workflow executions, "
    "all data objects that are outputs of deleted workflow executions, "
    "and all job records that have the workflow execution ID as their config.activity_id.",
)
async def delete_workflow_execution(
    workflow_execution_id: Annotated[
        str,
        Path(
            title="Workflow Execution ID",
            description="The `id` of the `WorkflowExecution` you want to delete.\n\n_Example_: `nmdc:wfmgan-11-abc123.1`",
            examples=["nmdc:wfmgan-11-abc123.1"],
        ),
    ],
    user: User = Depends(get_current_active_user),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    Delete a given workflow execution and its downstream workflow executions, data objects,
    functional annotation aggregation members, and related job records.

    This endpoint performs recursive deletion of:
    1. The specified workflow execution
    2. All downstream workflow executions that depend on this execution's outputs
    3. All functional annotation aggregation members generated by deleted workflow executions
    4. All data objects that are outputs of deleted workflow executions
    5. All job records that have the workflow execution ID as their config.activity_id

    Input data objects (has_input) are preserved as they may be used by other workflow executions.
    TODO: Consider deleting input data objects that are _not_ used by other workflow executions
          (otherwise, they may accumulate in the database as so-called "orphaned documents").

    TODO: Use a MongoDB transaction for this API endpoint, so we prevent the possibility that
          a `WorkflowExecution` be deleted via some other means before we have prepared the
          supersession chain accordingly, and the possibility that we prepare the supersession
          chain and then the deletion of the `WorkflowExecution` fails, and the possibility that
          we prepare the supersession chain and delete the `WorkflowExecution` but then the
          deletion of a downstream dependent `WorkflowExecution`, `DataObject`, etc. fails.

    Parameters
    ----------
    workflow_execution_id : str
        ID of the workflow execution to delete
    user : User
        Authenticated user (required)
    mdb : MongoDatabase
        MongoDB database connection

    Returns
    -------
    dict
        Catalog of deleted workflow executions, data objects, functional annotation aggregation members, and job records
    """

    # Check user permissions for delete operations
    # TODO: Decouple this endpoint's authorization criteria from that of the `/queries:run` endpoint.
    #       Currently, both endpoints rely on the "/queries:run(query_cmd:DeleteCommand)" allowance.
    check_can_update_and_delete(user)

    try:
        # Check if workflow execution exists
        workflow_execution = mdb.workflow_execution_set.find_one(
            {"id": workflow_execution_id}
        )
        if not workflow_execution:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Workflow execution {workflow_execution_id} not found",
            )

        # Track what we've deleted to avoid cycles and provide summary
        deleted_workflow_execution_ids: Set[str] = set()
        deleted_data_object_ids: Set[str] = set()
        deleted_functional_annotation_agg_oids: Set[str] = set()
        deleted_job_ids: Set[str] = set()

        def find_linked_workflow_executions(
            data_object_ids: List[str],
        ) -> List[str]:
            """Find workflow executions that use any of the given data objects as inputs."""
            if not data_object_ids:
                return []

            linked_wfes = list(
                mdb.workflow_execution_set.find(
                    {"has_input": {"$in": data_object_ids}}, {"id": 1}
                )
            )
            return [wfe["id"] for wfe in linked_wfes]

        def recursive_delete_workflow_execution(wfe_id: str) -> None:
            """Recursively delete a workflow execution and all its downstream dependents."""
            if wfe_id in deleted_workflow_execution_ids:
                return  # Already deleted or in progress

            # Get the workflow execution
            wfe = mdb.workflow_execution_set.find_one({"id": wfe_id})
            if not wfe:
                return  # Already deleted or doesn't exist

            # Mark as being processed to prevent cycles
            deleted_workflow_execution_ids.add(wfe_id)

            # Get output data objects from this workflow execution
            output_data_object_ids = wfe.get("has_output", [])

            # Check if this is an AnnotatingWorkflow (e.g., metagenome annotation)
            # If so, we need to also delete functional_annotation_agg records
            wfe_type = wfe.get("type", "")
            is_annotating_workflow = wfe_type in [
                MetagenomeAnnotation.class_class_curie,
                MetatranscriptomeAnnotation.class_class_curie,
                MetaproteomicsAnalysis.class_class_curie,
            ]

            # Find linked workflow executions that use these data objects as inputs
            linked_wfe_ids = find_linked_workflow_executions(output_data_object_ids)

            # Recursively delete linked workflow executions first
            for linked_wfe_id in linked_wfe_ids:
                if linked_wfe_id not in deleted_workflow_execution_ids:
                    recursive_delete_workflow_execution(linked_wfe_id)

            # Add data objects to deletion set
            deleted_data_object_ids.update(output_data_object_ids)

            # If this is an AnnotatingWorkflow, mark functional annotation records for deletion
            if is_annotating_workflow:
                func_annotation_records = list(
                    mdb.functional_annotation_agg.find(
                        {"was_generated_by": wfe_id}, {"_id": 1}
                    )
                )
                if func_annotation_records:
                    # Store the ObjectIds for deletion from functional_annotation_agg
                    deleted_functional_annotation_agg_oids.update(
                        [str(record["_id"]) for record in func_annotation_records]
                    )

            # Find and mark job records for deletion that have this workflow execution as activity_id
            job_records = list(mdb.jobs.find({"config.activity_id": wfe_id}, {"id": 1}))
            if job_records:
                deleted_job_ids.update([job["id"] for job in job_records])

        # Start recursive deletion from the target workflow execution
        recursive_delete_workflow_execution(workflow_execution_id)

        # Prepare deletion payload
        docs_to_delete = {}
        if deleted_workflow_execution_ids:
            docs_to_delete["workflow_execution_set"] = list(
                deleted_workflow_execution_ids
            )
        if deleted_data_object_ids:
            docs_to_delete["data_object_set"] = list(deleted_data_object_ids)
        if deleted_functional_annotation_agg_oids:
            docs_to_delete["functional_annotation_agg"] = list(
                deleted_functional_annotation_agg_oids
            )
        if deleted_job_ids:
            docs_to_delete["jobs"] = list(deleted_job_ids)

        # Perform the actual deletion using `_run_mdb_cmd`, so the operations
        # undergo schema, validation and referential integrity checking, and
        # deleted documents are backed up to the `nmdc_deleted` database.
        deletion_results = {}

        # For each `WorkflowExecution` that we will be deleting, update the `superseded_by` field
        # of each `WorkflowExecution` and `DataObject` that is superseded by it so that the
        # supersession chain remains intact.
        for wfe_id in deleted_workflow_execution_ids:
            workflow_execution = mdb["workflow_execution_set"].find_one({"id": wfe_id})
            if workflow_execution is not None:
                prepare_supersession_chain_for_workflow_execution_deletion(
                    workflow_execution=workflow_execution, db=mdb, client_session=None
                )
            else:
                logging.error(
                    f"WorkflowExecution '{wfe_id}' vanished during deletion prep."
                )
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="An error occurred while preparing to delete the WorkflowExecution.",
                )

        for collection_name, doc_ids in docs_to_delete.items():
            if not doc_ids:
                continue

            # Handle special case for functional_annotation_agg which uses _id instead of id
            if collection_name == "functional_annotation_agg":
                # Convert string ObjectIds back to ObjectId instances for the filter
                object_ids = [ObjectId(doc_id) for doc_id in doc_ids]
                filter_dict = {"_id": {"$in": object_ids}}
            else:
                # Standard case - use id field
                filter_dict = {"id": {"$in": doc_ids}}

            # Create delete command
            delete_cmd = DeleteCommand(
                delete=collection_name,
                deletes=[
                    DeleteStatement(q=filter_dict, limit=0)
                ],  # limit=0 means delete all matching
            )

            logging.warning(
                f"Executing cascading delete command for {collection_name} - you may temporarily encounter broken references."
            )
            # Execute the delete command
            if collection_name == "jobs":
                response = _run_delete_nonschema(delete_cmd, mdb)
            else:
                response = _run_mdb_cmd(delete_cmd, mdb, allow_broken_refs=True)

            # Store the result
            deletion_results[collection_name] = {
                "deleted_count": response.n,
                "doc_ids": doc_ids,
            }

        return {
            "message": "Workflow execution and dependencies deleted successfully",
            "deleted_workflow_execution_ids": list(deleted_workflow_execution_ids),
            "deleted_data_object_ids": list(deleted_data_object_ids),
            "deleted_functional_annotation_agg_oids": [
                str(oid) for oid in deleted_functional_annotation_agg_oids
            ],
            "deleted_job_ids": list(deleted_job_ids),
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(
            f"Error during workflow execution deletion: {str(e)}", exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error during deletion: {str(e)}",
        )
