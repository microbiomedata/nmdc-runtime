from ctypes import Union

from fastapi import APIRouter, Response, status

from nmdc_runtime.api.models.object import (
    DrsId,
    DrsObjectBlobIn,
    Error,
    DrsObjectIn,
)

router = APIRouter()


@router.post("/objects")
def create_object(object_in: DrsObjectIn, response: Response):
    if not isinstance(object_in, DrsObjectBlobIn):
        response.status_code = status.HTTP_501_NOT_IMPLEMENTED
        return Error(msg="Only blob creation supported at this time", status_code=501)
    # TODO: check authorization for, and use, object_in.site_id


@router.get("/objects")
def list_objects():
    pass


@router.get("/objects/{object_id}")
def get_object_info(object_id: DrsId):
    return object_id


@router.get("/objects/{object_id}/access/{access_id}")
def get_object_access(object_id: DrsId, access_id: str):
    return {"object_id": object_id, "access_id": access_id}


@router.patch("/objects/{object_id}")
def update_object():
    pass


@router.put("/objects/{object_id}")
def replace_object():
    pass
