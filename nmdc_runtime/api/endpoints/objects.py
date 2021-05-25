from typing import Union, List

from fastapi import APIRouter, Response, status, Depends

from nmdc_runtime.api.models.object import (
    DrsId,
    Error,
    DrsObject,
    AccessURL,
)
from nmdc_runtime.api.models.object_type import ObjectType
from nmdc_runtime.api.models.user import User, get_current_active_user

router = APIRouter()


@router.post("/objects", status_code=201, response_model=DrsObject)
def create_object(
    object_in: DrsObject,
    user: User = Depends(get_current_active_user),
):
    pass


@router.get("/objects", response_model=List[DrsObject])
def list_objects():
    pass


@router.get("/objects/{object_id}", response_model=DrsObject)
def get_object_info(object_id: DrsId):
    return object_id


@router.get("/objects/{object_id}/types", response_model=List[ObjectType])
def list_object_types(object_id: DrsId):
    return object_id


@router.put("/objects/{object_id}/types", response_model=List[ObjectType])
def replace_object_types(object_type_ids: List[str]):
    return object_type_ids


@router.get("/objects/{object_id}/access/{access_id}", response_model=AccessURL)
def get_object_access(object_id: DrsId, access_id: str):
    return {"object_id": object_id, "access_id": access_id}


@router.patch("/objects/{object_id}", response_model=DrsObject)
def update_object():
    pass


@router.put("/objects/{object_id}", response_model=DrsObject)
def replace_object():
    pass
