from typing import List

from fastapi import APIRouter, Depends

from nmdc_runtime.mongo_util import get_runtime_mdb, RuntimeAsyncMongoDatabase
from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.models.trigger import Trigger

router = APIRouter()


@router.get("/triggers", response_model=List[Trigger])
def list_triggers(
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
):
    return list(mdb.triggers.find())


@router.get("/triggers/{trigger_id}", response_model=Trigger)
def get_trigger(
    trigger_id: str,
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
):
    return raise404_if_none(mdb.triggers.find_one({"id": trigger_id}))
