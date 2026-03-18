import logging
import os
import re
from typing import Any, Dict, List, Set, Annotated, Tuple

import pymongo
from bson import ObjectId
from fastapi import APIRouter, Body, Depends, HTTPException, Path
from pymongo.database import Database as MongoDatabase
from pymongo.errors import BulkWriteError
from starlette import status

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.endpoints.lib.workflow_executions import (
    derive_predecessor_id,
    derive_successor_id,
    make_pattern_matching_ids_having_base_id,
    parse_workflow_execution_id,
    prepare_supersession_chain_for_workflow_execution_deletion,
)
from nmdc_runtime.api.endpoints.queries import (
    _run_mdb_cmd,
    check_can_update_and_delete,
    _run_delete_nonschema,
)
from nmdc_runtime.api.db.mongo import get_mongo_db, validate_json
from nmdc_runtime.api.models.capability import Capability
from nmdc_runtime.api.models.object_type import ObjectType
from nmdc_runtime.api.models.query import DeleteCommand, DeleteStatement
from nmdc_runtime.api.models.site import Site, get_current_client_site
from nmdc_runtime.api.models.user import User, get_current_active_user
from nmdc_runtime.api.models.util import DeleteResponse
from nmdc_runtime.api.models.workflow import Workflow
from nmdc_runtime.site.resources import MongoDB
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
    site: Site = Depends(get_current_client_site),
    mdb: MongoDatabase = Depends(get_mongo_db),
) -> Dict[str, str]:
    """
    Create workflow executions and related metadata.

    TODO: Critical!! This endpoint is currently accessible to any site client. It does not perform
          any authorization checks beyond that. I assume this was not intentional.

    TODO: Warning! This endpoint can currently be used to submit _all types of metadata_! Since we
          still use Mongo and rely on validation at the application level, keep this endpoint in
          mind when introducing new validation processes.

    High-level algorithm:
    1. Determine whether each submitted WFE is (by its `id`) superseded by any co-submitted WFE
       _or_ any existing WFE (where "existing" means "in the Mongo database"). If so, update the
       `superseded_by` fields in the insertion payload accordingly—both in the payload's
       `workflow_execution_set` collection and in its `data_object_set` collection, if any.
    2. Determine whether each submitted WFE (by its `id`) supersedes any existing WFE. If so,
       update the `superseded_by` fields in the database accordingly—both in the database's
       `workflow_execution_set` collection and in its `data_object_set` collection.
       Note: If the submitted WFE supersedes a co-submitted WFE, we would already have handled that
             in step 1 in step 1 when encountering the relationship from the other direction.
    3. Proceed to do the insertion of the [manipulated] payload.

    TODO: Perform all updates within a Mongo transaction. This may involve performing the insertions
    #     within this function, rather than delegating them to Dagster. As things are implemented
    #     now, the database could "change" between when we form our agenda and when we carry it out.

    Reference: https://microbiomedata.github.io/nmdc-schema/superseded_by/
    """

    _ = site  # must be authenticated

    # Get references to relevant MongoDB collections.
    workflow_execution_set = mdb.get_collection("workflow_execution_set")
    data_object_set = mdb.get_collection("data_object_set")

    # Do preliminary validation before we examine the submitted `workflow_execution_set` documents.
    # That way, our examination code can focus purely on examination and not validation.
    if (
        not validate_json(database_in, mdb, check_inter_document_references=False)
        or "workflow_execution_set" not in database_in
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Request payload must represent a valid 'nmdc:Database' instance "
                "that includes a 'workflow_execution_set' collection."
            ),
        )
    
    submitted_wfes = database_in["workflow_execution_set"]  # concise alias

    submitted_wfe_ids: List[str] = []
    existing_wfe_ids: List[str] = []

    with duration_logger(logging.info, "Gathering IDs that influence `superseded_by` fields"):
        # Extract the `base_id` and `run_number` from each submitted `WorkflowExecution`'s `id`;
        # raising an error if any of them lacks a `run_number` (since our workflow automation
        # system currently relies upon run numbers, despite the schema currently saying they are
        # optional and the minter currently minting `id`s that lack them).
        submitted_wfe_ids = [submitted_wfe["id"] for submitted_wfe in submitted_wfes]
        logging.info(f"{submitted_wfe_ids=}")
        id_parts_by_submitted_wfe_id: Dict[str, Tuple[str, int | None]] = {}
        for submitted_wfe_id in submitted_wfe_ids:
            base_id, run_number = parse_workflow_execution_id(submitted_wfe_id)
            if run_number is None:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        f"The ID of WorkflowExecution '{submitted_wfe_id}' is not in the "
                        "format required by our supersession management processes."
                    )
                )
            id_parts_by_submitted_wfe_id[submitted_wfe_id] = (base_id, run_number)

        # Now that we have all the `base_id` values, get the `id` of every `WorkflowExecution`
        # in the database whose `id` has any of those (submitted) `base_id` values. By getting
        # them all at once, we reduce the number of queries we have to run later, as we determine
        # the supersession relationships involving the submitted WFEs.
        base_id_patterns = []
        for _, (base_id, _) in id_parts_by_submitted_wfe_id.items():
            base_id_pattern: str = make_pattern_matching_ids_having_base_id(base_id)
            if base_id_pattern not in base_id_patterns:  # prevents duplicates
                base_id_patterns.append(base_id_pattern)
        filter_ = {"id": {"$regex": "|".join(base_id_patterns)}}  # join the patterns via "|"
        projection = {"id": 1, "_id": 0}
        existing_wfe_ids = [wfe["id"] for wfe in workflow_execution_set.find(filter_, projection)]
        logging.info(f"{existing_wfe_ids=}")

        # If the `id` of any submitted `WorkflowExecution` matches the `id` of any existing
        # `WorkflowExecution`, abort. That way, we don't update the supersession chains in
        # preparation for an insertion that is destined to fail.
        for submitted_wfe_id in submitted_wfe_ids:
            if submitted_wfe_id in existing_wfe_ids:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"WorkflowExecution '{submitted_wfe_id}' has the same ID as an existing one."
                    ),
                )

    with duration_logger(logging.info, "Patching `superseded_by` fields in submitted payload"):
        for submitted_wfe_id in submitted_wfe_ids:
            # Check whether this `WorkflowExecution` is superseded by any `WorkflowExecution`s.
            successor_wfe_id = derive_successor_id(submitted_wfe_id)
            if successor_wfe_id in submitted_wfe_ids or successor_wfe_id in existing_wfe_ids:
                logging.info(
                    f"WorkflowExecution '{submitted_wfe_id}' is superseded by "
                    f"co-submitted or existing WorkflowExecution '{successor_wfe_id}'."
                )
                # Update the insertion payload accordingly.
                # TODO: Update the `DataObject`s also.
                for submitted_wfe in submitted_wfes:
                    if submitted_wfe["id"] == submitted_wfe_id:
                        submitted_wfe["superseded_by"] = successor_wfe_id
            else:
                logging.info(
                    f"WorkflowExecution '{submitted_wfe_id}' is not superseded by "
                    "any co-submitted or existing WorkflowExecution."
                )

    with duration_logger(logging.info, "Updating `superseded_by` fields in database"):
        for submitted_wfe_id in submitted_wfe_ids:
            # Check whether this `WorkflowExecution` supersedes any `WorkflowExecution`s.
            predecessor_wfe_id = derive_predecessor_id(submitted_wfe_id)
            if predecessor_wfe_id in existing_wfe_ids:
                logging.info(
                    f"WorkflowExecution '{submitted_wfe_id}' supersedes "
                    f"existing WorkflowExecution '{predecessor_wfe_id}'."
                )
                # Update the database accordingly.
                # TODO: Update the `DataObject`s also.
                workflow_execution_set.update_one(
                    {"id": predecessor_wfe_id},
                    {"$set": {"superseded_by": submitted_wfe_id}},
                )

    try:
        # validate request JSON
        rv = validate_json(database_in, mdb, check_inter_document_references=True)
        if rv["result"] == "errors":
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(rv),
            )
        # create mongodb instance for dagster
        mongo_resource = MongoDB(
            host=os.getenv("MONGO_HOST"),
            dbname=os.getenv("MONGO_DBNAME"),
            username=os.getenv("MONGO_USERNAME"),
            password=os.getenv("MONGO_PASSWORD"),
        )
        mongo_resource.add_docs(database_in, validate=False, replace=True)
        return {"message": "jobs accepted"}
    except BulkWriteError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))


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
