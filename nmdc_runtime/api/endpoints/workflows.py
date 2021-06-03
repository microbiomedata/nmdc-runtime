from typing import List

import pymongo
from fastapi import APIRouter, Depends

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.capability import Capability
from nmdc_runtime.api.models.object_type import ObjectType
from nmdc_runtime.api.models.workflow import Workflow

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
