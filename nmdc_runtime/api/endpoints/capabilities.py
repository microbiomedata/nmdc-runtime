from typing import List, Annotated

import pymongo
from fastapi import APIRouter, Depends, Path

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.capability import Capability

router = APIRouter()


@router.get("/capabilities", response_model=List[Capability])
def list_capabilities(
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return list(mdb.capabilities.find())


@router.get("/capabilities/{capability_id}", response_model=Capability)
def get_capability(
    capability_id: Annotated[
        str,
        Path(
            title="Capability ID",
            description="The unique identifier of the capability.",
            examples=["cap-123"],
        ),
    ],
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return raise404_if_none(mdb.capabilities.find_one({"id": capability_id}))
