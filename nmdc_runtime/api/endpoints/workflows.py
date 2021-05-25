from datetime import datetime, timezone
from typing import List

import pymongo
from fastapi import APIRouter, Depends, status
from pydantic import BaseModel
from starlette.responses import JSONResponse

from nmdc_runtime.api.core.idgen import generate_id_unique
from nmdc_runtime.api.core.util import pick
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.workflow import Workflow, WorkflowBase

router = APIRouter()


@router.post("/workflows", response_model=Workflow)
def create_workflow(
    workflow_base: WorkflowBase,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    wfid = generate_id_unique(mdb, "wf")
    workflow = Workflow(
        **workflow_base.dict(), id=wfid, created_at=datetime.now(timezone.utc)
    )
    mdb.workflows.insert_one(workflow.dict())
    # TODO move below to a db bootstrap routine
    mdb.workflows.create_index("id")
    return workflow


@router.get("/workflows/{workflow_id}", response_model=Workflow)
def get_workflow(
    workflow_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    doc = mdb.workflows.find_one({"id": workflow_id})
    if not doc:
        return JSONResponse(content={}, status_code=status.HTTP_404_NOT_FOUND)
    return doc


@router.get("/workflows", response_model=List[Workflow])
def list_workflows(
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return list(mdb.workflows.find())


@router.delete("/workflows/{workflow_id}", response_model=BaseModel)
def delete_workflow(
    workflow_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    result = mdb.workflows.delete_one({"id": workflow_id})
    if result.deleted_count == 0:
        return JSONResponse(content={}, status_code=status.HTTP_404_NOT_FOUND)
    return {}
