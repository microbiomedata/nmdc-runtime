from typing import List

import pymongo
from fastapi import APIRouter, Depends

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.object_type import ObjectType
from nmdc_runtime.api.models.workflow import Workflow

router = APIRouter()


@router.get("/object_types", response_model=List[ObjectType])
def list_object_types(
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return list(mdb.object_types.find())


@router.get("/object_types/{object_type_id}", response_model=ObjectType)
def get_object_type(
    object_type_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return raise404_if_none(mdb.object_types.find_one({"id": object_type_id}))


@router.get("/object_types/{object_type_id}/workflows", response_model=List[Workflow])
def list_object_type_workflows(
    object_type_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    workflow_ids = [
        doc["workflow_id"]
        for doc in mdb.triggers.find({"object_type_id": object_type_id})
    ]
    return list(mdb.workflows.find({"id": {"$in": workflow_ids}}))
