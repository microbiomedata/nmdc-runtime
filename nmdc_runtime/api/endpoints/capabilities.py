from typing import List, Annotated

import pymongo
from fastapi import APIRouter, Depends, Path

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.capability import Capability

router = APIRouter()


@router.get(
    "/capabilities", 
    response_model=List[Capability],
    description="List all available capabilities",
)
def list_capabilities(
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    """
    Retrieve a list of all capabilities available in the system.
    
    Capabilities define the functional requirements for workflow execution.
    """
    return list(mdb.capabilities.find())


@router.get(
    "/capabilities/{capability_id}", 
    response_model=Capability,
    description="Get details of a specific capability",
)
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
    """
    Retrieve detailed information about a specific capability.
    
    Returns the capability definition including its requirements and configuration.
    """
    return raise404_if_none(mdb.capabilities.find_one({"id": capability_id}))
