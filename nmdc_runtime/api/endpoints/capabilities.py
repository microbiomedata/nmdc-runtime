from typing import List

from fastapi import APIRouter, Depends

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.mongo_util import get_runtime_mdb, RuntimeAsyncMongoDatabase
from nmdc_runtime.api.models.capability import Capability

router = APIRouter()


@router.get("/capabilities", response_model=List[Capability])
async def list_capabilities(
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
):
    return await mdb.raw.get_collection("capabilities").find().to_list()


@router.get("/capabilities/{capability_id}", response_model=Capability)
async def get_capability(
    capability_id: str,
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
):
    return raise404_if_none(
        await mdb.raw.get_collection("capabilities").find_one({"id": capability_id})
    )
