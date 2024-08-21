import os
from typing import Any, List

import pymongo

from fastapi import APIRouter, Depends, HTTPException
from pymongo.database import Database as MongoDatabase
from pymongo.errors import BulkWriteError
from starlette import status

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.capability import Capability
from nmdc_runtime.api.models.object_type import ObjectType
from nmdc_runtime.api.models.site import Site, get_current_client_site
from nmdc_runtime.api.models.workflow import Workflow
from nmdc_runtime.site.resources import MongoDB
from nmdc_runtime.util import validate_json

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


@router.post("/workflows/activities", status_code=410, deprecated=True)
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
        rv = validate_json(workflow_execution_set, mdb)
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
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
