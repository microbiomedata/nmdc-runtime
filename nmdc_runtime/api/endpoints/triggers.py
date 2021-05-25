from datetime import datetime, timezone
from typing import List

import pymongo
from fastapi import APIRouter, Depends, status
from pydantic import BaseModel
from starlette.responses import JSONResponse

from nmdc_runtime.api.core.idgen import generate_id_unique
from nmdc_runtime.api.core.util import pick
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.trigger import Trigger, TriggerBase

router = APIRouter()


@router.post("/triggers", response_model=Trigger)
def create_trigger(
    trigger_base: TriggerBase,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    doc = mdb.triggers.find_one(
        pick(["workflow_id", "object_type_id"], trigger_base.dict())
    )
    if doc:
        return JSONResponse(
            content="trigger exists", status_code=status.HTTP_409_CONFLICT
        )
    tid = generate_id_unique(mdb, "tr")
    trigger = Trigger(
        **trigger_base.dict(), id=tid, created_at=datetime.now(timezone.utc)
    )
    mdb.triggers.insert_one(trigger.dict())
    # TODO mdb.triggers.create_index("id")
    return trigger


@router.get("/triggers/{trigger_id}", response_model=Trigger)
def get_trigger(
    trigger_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    doc = mdb.triggers.find_one({"id": trigger_id})
    if not doc:
        return JSONResponse(content={}, status_code=status.HTTP_404_NOT_FOUND)
    return doc


@router.get("/triggers", response_model=List[Trigger])
def list_triggers(
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return list(mdb.triggers.find())


@router.delete("/triggers/{trigger_id}", response_model=BaseModel)
def delete_trigger(
    trigger_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    result = mdb.triggers.delete_one({"id": trigger_id})
    if result.deleted_count == 0:
        return JSONResponse(content={}, status_code=status.HTTP_404_NOT_FOUND)
    return {}
