import logging
import os
from typing import Any, List, Set, Annotated

import pymongo
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Path
from pymongo.database import Database as MongoDatabase
from pymongo.errors import BulkWriteError
from starlette import status

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.endpoints.queries import _run_mdb_cmd, check_can_update_and_delete
from nmdc_runtime.api.db.mongo import get_mongo_db, validate_json
from nmdc_runtime.api.models.capability import Capability
from nmdc_runtime.api.models.object_type import ObjectType
from nmdc_runtime.api.models.query import DeleteCommand, DeleteStatement
from nmdc_runtime.api.models.site import Site, get_current_client_site
from nmdc_runtime.api.models.user import User, get_current_active_user
from nmdc_runtime.api.models.util import DeleteResponse
from nmdc_runtime.api.models.workflow import Workflow
from nmdc_runtime.site.resources import MongoDB


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


@router.post("/workflows/workflow_executions")
async def post_workflow_execution(
    workflow_execution_set: dict[str, Any],
    site: Site = Depends(get_current_client_site),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    Post workflow execution set to database and claim job.

    Parameters
    -------
    workflow_execution_set: dict[str,Any]
             Set of workflow executions for specific workflows, in the form of a nmdc:Database.
             Other collections (such as data_object_set) are allowed, as they may be associated
             with the workflow executions submitted.

    site: Site
    mdb: MongoDatabase

    Returns
    -------
    dict[str,str]

    """
    _ = site  # must be authenticated
    try:
        # validate request JSON
        rv = validate_json(
            workflow_execution_set, mdb, check_inter_document_references=True
        )
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
        mongo_resource.add_docs(workflow_execution_set, validate=False, replace=True)
        return {"message": "jobs accepted"}
    except BulkWriteError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))


@router.delete(
    "/workflows/workflow_executions/{workflow_execution_id}",
    response_model=DeleteResponse,
    description="Delete a workflow execution and cascade to downstream workflow executions and data objects. "
    "This endpoint performs recursive deletion of the specified workflow execution, "
    "all downstream workflow executions that depend on this workflow execution's outputs, "
    "and all data objects that are outputs of deleted workflow executions. ",
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
    and functional annotation aggregation members.

    This endpoint performs recursive deletion of:
    1. The specified workflow execution
    2. All downstream workflow executions that depend on this execution's outputs
    3. All data objects that are outputs of deleted workflow executions

    Input data objects (has_input) are preserved as they may be used by other workflow executions.

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
        Catalog of deleted workflow executions, data objects, and functional annotation aggregation members
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
            """Recursively delete a workflow execution and all its downstream dependencies."""
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
                "nmdc:MetagenomeAnnotation",
                "nmdc:MetatranscriptomeAnnotation",
                "nmdc:MetaproteomicsAnalysis",
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

        # Perform the actual deletion using `_run_mdb_cmd`, so the operations
        # undergo schema, validation and referential integrity checking, and
        # deleted documents are backed up to the `nmdc_deleted` database.
        deletion_results = {}

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
