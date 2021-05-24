from typing import Union

import botocore
import pymongo.database
from fastapi import APIRouter, Response, status, Depends

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.db.s3 import get_s3_client, presigned_url_to_put
from nmdc_runtime.api.models.object import (
    DrsId,
    DrsObjectBlobIn,
    Error,
    DrsObjectPresignedUrlPut,
    DrsObjectBundleIn,
)
from nmdc_runtime.api.models.user import User, get_current_active_user

router = APIRouter()


@router.post("/objects", status_code=202)
def create_object(
    object_in: Union[DrsObjectBlobIn, DrsObjectBundleIn],
    response: Response,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
    s3client: botocore.client.BaseClient = Depends(get_s3_client),
    user: User = Depends(get_current_active_user),
):
    if not isinstance(object_in, DrsObjectBlobIn):
        response.status_code = status.HTTP_501_NOT_IMPLEMENTED
        return Error(msg="Only blob creation supported at this time", status_code=501)
    site = mdb.sites.find_one({"id": object_in.site_id})
    if site is None:
        response.status_code = 404
        return Error(msg=f"no site with ID '{object_in.site_id}'", status_code=404)
    expires_in = 300
    # TODO generate ID
    # TODO dagster sensor polls for new bucket files, creates object resources in terminus.
    url = presigned_url_to_put("myfile.json", client=s3client, expires_in=expires_in)
    return DrsObjectPresignedUrlPut(**{"url": url, "expires_in": expires_in})


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
