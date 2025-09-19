from typing import List, Annotated

import pymongo
from fastapi import APIRouter, Depends, Path

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
    trigger_id: Annotated[
        str,
        Path(
            title="Trigger ID",
            description="The unique identifier of the trigger.",
            examples=["trigger-123"],
        ),
    ],
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return raise404_if_none(mdb.triggers.find_one({"id": trigger_id}))
