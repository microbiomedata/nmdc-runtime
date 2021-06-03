from typing import List

import pymongo
from fastapi import APIRouter, Depends

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.trigger import Trigger

router = APIRouter()


@router.get("/triggers", response_model=List[Trigger])
def list_triggers(
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return list(mdb.triggers.find())


@router.get("/triggers/{trigger_id}", response_model=Trigger)
def get_trigger(
    trigger_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return raise404_if_none(mdb.triggers.find_one({"id": trigger_id}))
