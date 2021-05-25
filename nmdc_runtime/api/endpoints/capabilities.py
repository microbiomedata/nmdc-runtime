from datetime import datetime, timezone
from typing import List

import pymongo
from fastapi import APIRouter, Depends, status
from pydantic import BaseModel
from starlette.responses import JSONResponse

from nmdc_runtime.api.core.idgen import generate_id_unique
from nmdc_runtime.api.core.util import pick
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.capability import Capability, CapabilityBase

router = APIRouter()


@router.post("/capabilities", response_model=Capability)
def create_capability(
    capability_base: CapabilityBase,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    cpid = generate_id_unique(mdb, "cp")
    capability = Capability(
        **capability_base.dict(), id=cpid, created_at=datetime.now(timezone.utc)
    )
    mdb.capabilities.insert_one(capability.dict())
    # TODO move below to a db bootstrap routine
    mdb.capabilities.create_index("id")
    return capability


@router.get("/capabilities/{capability_id}", response_model=Capability)
def get_capability(
    capability_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    doc = mdb.capabilities.find_one({"id": capability_id})
    if not doc:
        return JSONResponse(content={}, status_code=status.HTTP_404_NOT_FOUND)
    return doc


@router.get("/capabilities", response_model=List[Capability])
def list_capabilities(
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return list(mdb.capabilities.find())


@router.delete("/capabilities/{capability_id}", response_model=BaseModel)
def delete_capability(
    capability_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    result = mdb.capabilities.delete_one({"id": capability_id})
    if result.deleted_count == 0:
        return JSONResponse(content={}, status_code=status.HTTP_404_NOT_FOUND)
    return {}
