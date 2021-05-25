from datetime import datetime, timezone
from typing import List

import pymongo
from fastapi import APIRouter, Depends, status
from pydantic import BaseModel
from starlette.responses import JSONResponse

from nmdc_runtime.api.core.idgen import generate_id_unique
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.object_type import ObjectType, ObjectTypeBase

router = APIRouter()


@router.post("/object_types", response_model=ObjectType)
def create_object_type(
    object_type_base: ObjectTypeBase,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    otid = generate_id_unique(mdb, "ot")
    object_type = ObjectType(
        **object_type_base.dict(), id=otid, created_at=datetime.now(timezone.utc)
    )
    mdb.object_types.insert_one(object_type.dict())
    # TODO move below to a db bootstrap routine
    mdb.object_types.create_index("id")
    return object_type


@router.get("/object_types/{object_type_id}", response_model=ObjectType)
def get_object_type(
    object_type_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    doc = mdb.object_types.find_one({"id": object_type_id})
    if not doc:
        return JSONResponse(content={}, status_code=status.HTTP_404_NOT_FOUND)
    return doc


@router.get("/object_types", response_model=List[ObjectType])
def list_object_types(
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return list(mdb.object_types.find())


@router.delete("/object_types/{object_type_id}", response_model=BaseModel)
def delete_object_type(
    object_type_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    result = mdb.object_types.delete_one({"id": object_type_id})
    if result.deleted_count == 0:
        return JSONResponse(content={}, status_code=status.HTTP_404_NOT_FOUND)
    return {}
