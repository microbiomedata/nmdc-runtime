from typing import List, Annotated

import pymongo
from fastapi import APIRouter, Depends, Path

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.object_type import ObjectType
from nmdc_runtime.api.models.workflow import Workflow

router = APIRouter()


@router.get(
    "/object_types",
    response_model=List[ObjectType],
    description="List all available object types",
)
def list_object_types(
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    """
    Retrieve a list of all object types available in the system.

    Object types define the categories of data objects that can trigger workflows.
    """
    return list(mdb.object_types.find())


@router.get(
    "/object_types/{object_type_id}",
    response_model=ObjectType,
    description="Get details of a specific object type",
)
def get_object_type(
    object_type_id: Annotated[
        str,
        Path(
            title="Object Type ID",
            description="The unique identifier of the object type.",
            examples=["nmdc:DataObject"],
        ),
    ],
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    """
    Retrieve detailed information about a specific object type.

    Returns the object type definition including its properties and workflow associations.
    """
    return raise404_if_none(mdb.object_types.find_one({"id": object_type_id}))


@router.get(
    "/object_types/{object_type_id}/workflows",
    response_model=List[Workflow],
    description="List workflows triggered by an object type",
)
def list_object_type_workflows(
    object_type_id: Annotated[
        str,
        Path(
            title="Object Type ID",
            description="The unique identifier of the object type whose workflows to list.",
            examples=["nmdc:DataObject"],
        ),
    ],
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    """
    List all workflows that can be triggered by the specified object type.

    Returns workflow definitions that are configured to execute when objects of this type are created.
    """
    workflow_ids = [
        doc["workflow_id"]
        for doc in mdb.triggers.find({"object_type_id": object_type_id})
    ]
    return list(mdb.workflows.find({"id": {"$in": workflow_ids}}))
